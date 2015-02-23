/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>

#include <mesos/executor.hpp>

#include <stout/duration.hpp>
#include <stout/os.hpp>

#include <unistd.h>

using namespace std;
using namespace mesos;

struct ExternalInfo {
  int pid;
  bool failed;
};

static ExternalInfo StartExternalProcess (const string& path) {
  ExternalInfo info = { 0, true };
  int processPid;

  processPid = fork();

  // child process
  if (processPid == 0) {
    close(0);
    fcntl(1, F_SETFD, 0);
    fcntl(2, F_SETFD, 0);

    // ignore signals in worker process
    signal(SIGINT,  SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGUSR1, SIG_IGN);

    // execute worker
    execlp(path.c_str(), path.c_str(), "300", nullptr);

    _exit(1);
  }

  // parent
  if (processPid == -1) {
    cerr << "fork failed\n";

    info.failed = true;
    return info;
  }

  info.pid = processPid;
  info.failed = false;

  return info;
}

struct StartInfo {
  StartInfo (ExecutorDriver* driver, const TaskInfo& task) 
    : driver(driver), task(task) {
  }

  ExecutorDriver* driver;
  const TaskInfo task;
};

static void* RunProcess (void* args) {
  StartInfo* info = static_cast<StartInfo*>(args);
  ExecutorDriver* driver = info->driver;
  const TaskInfo& task = info->task;

  ExternalInfo external = StartExternalProcess("/bin/sleep");

  {
    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());

    if (external.failed) {
      status.set_state(TASK_FAILED);
      driver->sendStatusUpdate(status);
      delete info;
      return nullptr;
    }

    cout << "PID " << external.pid << "\n";

    status.set_state(TASK_RUNNING);
    driver->sendStatusUpdate(status);
  }

  TaskStatus status;
  status.mutable_task_id()->MergeFrom(task.task_id());

  int s;
  waitpid(external.pid, &s, WUNTRACED);

  cout << "WAIT " << external.pid << " returned\n";

  if (WIFEXITED(s)) {
    if (WEXITSTATUS(s) == 0) {
      cout << "EXIT " << external.pid << " status == 0\n";
      status.set_state(TASK_FINISHED);
    }
    else {
      cout << "EXIT " << external.pid << " status != 0\n";
      status.set_state(TASK_FAILED);
    }
  }
  else if (WIFSIGNALED(s)) {
    cout << "EXIT " << external.pid << " signalled\n";
    status.set_state(TASK_FAILED);
  }
  else if (WIFSTOPPED(s)) {
    cout << "EXIT " << external.pid << " stopped\n";
    // deal with stopped, but how?
    kill(external.pid, 9);
    status.set_state(TASK_FAILED);
  }

  driver->sendStatusUpdate(status);

  return nullptr;
}


class TestExecutor : public Executor
{
public:
  virtual ~TestExecutor() {}

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
    cout << "Registered executor on " << slaveInfo.hostname() << endl;
  }

  virtual void reregistered(ExecutorDriver* driver,
                            const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered executor on " << slaveInfo.hostname() << endl;
  }

  virtual void disconnected(ExecutorDriver* driver) {
    cout << "disconnected\n";
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task) {
    cout << "Starting task " << task.task_id().value() << endl;

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());

    StartInfo* info = new StartInfo(driver, task);

    pthread_t pthread;
    int res = pthread_create(&pthread, NULL, &RunProcess, info);

    if (res != 0) {
      status.set_state(TASK_FAILED);
    } 
    else {
      pthread_detach(pthread);
      status.set_state(TASK_RUNNING);
    }

    driver->sendStatusUpdate(status);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
    cout << "killTask\n";
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {
    cout << "frameworkMessage\n";
  }

  virtual void shutdown(ExecutorDriver* driver) {
    cout << "shutdown\n";
  }

  virtual void error(ExecutorDriver* driver, const string& message) {
    cout << "error: " << message << "\n";
  }
};


int main(int argc, char** argv)
{
  cout << "starting executor\n";

  TestExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
