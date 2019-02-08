let loadtestService = require('./loadtest_service.js');
let grpc = require('grpc');
let metrics_tracker = require('./metrics_tracker.js');
let os = require('os');
const cp = require('child_process');
let SettablePromise = require('./settable_promise.js');

class SubtaskWorker {
    constructor() {
    }

    start(startRequest) {
        this.metricsTracker = new metrics_tracker.MetricsTracker(startRequest.include_ids);
        this.childStart(startRequest);
    }

    // Returns void
    childStart(startRequest) {
        throw new Error('Unimplemented.');
    }

    check() {
        return this.metricsTracker.check();
    }

    startHandler(call, callback) {
        callback(null, this.start(call.request));
    }

    checkHandler(call, callback) {
        callback(null, this.check());
    }
}

class TaskWorker {
    constructor(workerScript) {
        this.worker = cp.fork(workerScript);
        this.initDone = new SettablePromise();
        this.startDone = new SettablePromise();
    }

    static runWorker(subtaskWorker) {
        let server = new grpc.Server();
        server.addService(loadtestService.LoadtestWorker.service, {
            Start: subtaskWorker.startHandler.bind(subtaskWorker),
            Check: subtaskWorker.checkHandler.bind(subtaskWorker)
        });
        let port = server.bind('localhost:0', grpc.ServerCredentials.createInsecure());
        server.start();
        process.send(port);
        console.log("Launched worker at port " + port);
    }

    async init() {
        await new Promise(resolve => {
            this.worker.once("message", port => {
                console.log("received worker port: " + port);
                this.port = port;
                resolve(null);
            });
        });
        this.initDone.set();
    }

    async start(startRequest) {
        await this.initDone.promise;
        let address = 'localhost:' + this.port;
        console.log("worker start at:", address);
        this.stub = new loadtestService.LoadtestWorker(
            address, grpc.credentials.createInsecure());
        let promise = new Promise((resolve, reject) => {
            this.stub.Start(startRequest, (error, _) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(null);
            });
        });
        await promise;
        console.log("worker start done");
        this.startDone.set();
    }

    stop() {
        this.stub = undefined;
        this.worker.kill();
    }

    async check() {
        await this.startDone.promise;
        let result = await new Promise((resolve, reject) => {
            if (undefined === this.stub) {
                resolve({
                    bucket_values: [],
                    received_messages: []
                });
                return;
            }
            this.stub.Check({}, (error, checkResponse) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(checkResponse);
            })
        });
        return result;
    }
}

class Task {
    constructor() {
        let cores = os.cpus().length;
        this.workers = [];
        for (const {} of Array(cores).keys()) {
            this.workers.push(this.getWorker())
        }
        this.finished = false;
    }

    getWorker() {
        throw new Error('Unimplemented.');
    }

    async init() {
        this.workers.forEach(async worker => {
            await worker.init();
        });
    }

    static toMillis(timeOrDuration) {
        return Math.round((timeOrDuration.seconds * 1000) +
            (timeOrDuration.nanos / 1000000.0));
    }

    async start(startRequest) {
        console.log("task start");
        this.startTime = Task.toMillis(startRequest.start_time);
        this.testDuration = Task.toMillis(startRequest.test_duration);
        if (startRequest.hasOwnProperty("publisher_options")) {
            let options = startRequest.publisher_options;
            options.rate /= this.workers.length;
        }
        let startPromises = [];
        this.workers.forEach(worker => {
            startPromises.push(worker.start(startRequest));
        });
        await Promise.all(startPromises);
        console.log("all started");
        setTimeout(() => {
            this.workers.forEach(worker => {
                worker.stop();
            });
            this.finished = true;
        }, (this.startTime + this.testDuration) - (new Date()).getTime());
    }

    millisSinceStart() {
        return (new Date()).getTime() - this.startTime;
    }

    // Returns a promise containing the CheckResponse
    async check() {
        let checkPromises = [];
        this.workers.forEach(worker => {
            checkPromises.push(worker.check());
        });
        let checkResults = await Promise.all(checkPromises);
        let combined = metrics_tracker.MetricsTracker.combineResponses(
            checkResults);
        combined.running_duration = {
            seconds: Math.floor(this.millisSinceStart() / 1000)
        };
        combined.is_finished = this.finished;
        return combined;
    }

    startHandler(call, callback) {
        this.start(call.request).then(() => {
            callback(null, {});
        });
    }

    checkHandler(call, callback) {
        this.check().then(checkResponse => {
            callback(null, checkResponse);
        });
    }
}

module.exports = {
    SubtaskWorker: SubtaskWorker,
    TaskWorker: TaskWorker,
    Task: Task
};
