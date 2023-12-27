#include <iostream>
#include <fstream>
#include <yaml-cpp/yaml.h>
#include <set>
#include <vector>
#include <string>
#include <sys/wait.h>
#include <queue>
#include <unordered_set>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

// Функция для обработки задачи и её зависимостей
void processJob(const std::string& job, const std::unordered_map<std::string, std::vector<std::string>>& dependencies,
                std::unordered_set<std::string>& visitedJobs) {
    if (visitedJobs.size() == dependencies.size()) {
        return;
    }

    if (visitedJobs.count(job) == 0) {
        visitedJobs.insert(job);
    }

    if (dependencies.count(job) > 0) {
        const std::vector<std::string>& currentDependencies = dependencies.at(job);

        for (const auto& dependency : currentDependencies) {
            if (visitedJobs.count(dependency) == 0) {
                visitedJobs.insert(dependency);
                if (visitedJobs.size() == dependencies.size()) {
                    std::cout << "Job done: " << dependency << std::endl;
                    sleep(1);
                } else {
                    processJob(dependency, dependencies, visitedJobs);
                }
            }
        }
    }

    std::cout << "Job done: " << job << std::endl;
    sleep(1);
}

// Функция для проверки наличия циклов в графе
bool hasCycle(const YAML::Node& jobs, const std::string& currentJob, std::set<std::string>& visited,
              std::set<std::string>& recursionStack) {
    visited.insert(currentJob);
    recursionStack.insert(currentJob);

    const YAML::Node& dependencies = jobs[currentJob]["dependencies"];
    for (const auto& dependency : dependencies) {
        const std::string& dependencyJob = dependency.as<std::string>();

        if (recursionStack.count(dependencyJob)) {
            return true;
        }

        if (!visited.count(dependencyJob) && hasCycle(jobs, dependencyJob, visited, recursionStack)) {
            return true;
        }
    }

    recursionStack.erase(currentJob);
    return false;
}

// Функция для проверки, что граф является направленным ациклическим графом (DAG)
bool isValidDAG(const YAML::Node& jobs) {
    std::set<std::string> visited;
    std::set<std::string> recursionStack;

    for (const auto& job : jobs) {
        if (!visited.count(job.first.as<std::string>()) &&
            hasCycle(jobs, job.first.as<std::string>(), visited, recursionStack)) {
            return false;
        }
    }

    return true;
}

// Функция для проверки, что граф имеет только одну компоненту связности
bool hasOnlyOneComponent(const YAML::Node& jobs) {
    std::unordered_map<std::string, std::vector<std::string>> adjacencyList;
    std::set<std::string> visited;

    // Строим список смежности
    for (const auto& job : jobs) {
        const std::string &jobName = job.first.as<std::string>();
        const YAML::Node &dependencies = job.second["dependencies"];
        for (const auto& dependency : dependencies) {
            adjacencyList[dependency.as<std::string>()].push_back(jobName);
            adjacencyList[jobName].push_back(dependency.as<std::string>());
        }
    }

    int componentCount = 0;

    // Обходим граф и считаем количество компонент связности
    for (const auto& job : jobs) {
        const std::string &jobName = job.first.as<std::string>();
        if (visited.count(jobName) == 0) {
            std::set<std::string> component;
            std::queue<std::string> q;

            q.push(jobName);
            visited.insert(jobName);

            while (!q.empty()) {
                std::string currentJob = q.front();
                q.pop();
                component.insert(currentJob);

                for (const auto& neighbor : adjacencyList[currentJob]) {
                    if (visited.count(neighbor) == 0) {
                        q.push(neighbor);
                        visited.insert(neighbor);
                    }
                }
            }

            componentCount++;
        }
    }

    return componentCount > 1;
}

// Функция для проверки наличия стартовых и завершающих задач в графе
bool hasStartAndEndJobs(const YAML::Node &jobs) {
    std::set<std::string> startJobs;
    std::set<std::string> endJobs;

    for (const auto& job : jobs) {
        const std::string &jobName = job.first.as<std::string>();
        const YAML::Node &dependencies = job.second["dependencies"];
        if (dependencies.IsNull() || dependencies.size() == 0) {
            startJobs.insert(jobName);
        }

        for (const auto& dependency : dependencies) {
            endJobs.erase(dependency.as<std::string>());
        }

        endJobs.insert(jobName);
    }

    return !startJobs.empty() && !endJobs.empty();
}

int main(int argc, char* argv[]) {
    // Получение имени файла из аргументов командной строки
    std::string fileName = argv[1];
    std::ifstream file(fileName);
    YAML::Node data = YAML::Load(file);
    YAML::Node jobs = data["jobs"];

    // Проверка наличия циклов в графе
    if (!isValidDAG(jobs)) {
        throw std::runtime_error("DAG contains a cycle");
    }

    // Проверка, что граф имеет только одну компоненту связности
    if (hasOnlyOneComponent(jobs)) {
        throw std::runtime_error("DAG has more than one connectivity component");
    }

    // Проверка наличия стартовых и завершающих задач в графе
    if (!hasStartAndEndJobs(jobs)) {
        throw std::runtime_error("DAG does not have a start and end job");
    }

    // Вывод сообщения о том, что граф является валидным DAG
    std::cout << "DAG is valid" << std::endl;
    sleep(1);

    // Получение количества задач
    int n = jobs.size();
    std::vector<std::string> jobNames;
    for (const auto& job : jobs) {
        jobNames.push_back(job.first.as<std::string>());
    }

    std::unordered_map<std::string, std::vector<std::string>> dependencies_map;
    std::unordered_set<std::string> visitedJobs;

    // Построение карты зависимостей
    for (const auto& job : jobs) {
        const std::string &jobName = job.first.as<std::string>();
        std::vector<std::string> dependencies;
        for (const auto& dependency : job.second["dependencies"]) {
            dependencies.push_back(dependency.as<std::string>());
        }
        dependencies_map[jobName] = dependencies;
    }

    // Обработка задач с учетом зависимостей
    for (const std::string& job : jobNames) {
        processJob(job, dependencies_map, visitedJobs);
    }

    std::cout << "All jobs done" << std::endl;

    return 0;
}
