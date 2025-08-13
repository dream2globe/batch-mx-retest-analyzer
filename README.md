# Retest Report Generator

## 1. Project Overview

This project analyzes data collected from inspectors in the manufacturing process to identify defect items with a high reproduction rate, thereby preventing unnecessary re-inspections. The analysis logic, originally developed in a Jupyter notebook, has been converted into a production-ready Python application that can be executed in a containerized environment.

## 2. Project Structure

The project is structured by function to ensure clarity and maintainability of the code:

- `main.py`: The entry point of the application, which orchestrates the overall workflow.
- `app/settings.py`: Manages application settings, including Spark and S3 configurations, using `pydantic-settings`.
- `app/spark.py`: Initializes and configures the Spark session.
- `app/data_loader.py`: Loads and preprocesses raw data from the data source.
- `app/analyzer.py`: Contains the core logic for analyzing retest and retry rates.
- `app/writer.py`: Writes the analysis results to the specified output location.
- `config.yml`: Contains non-sensitive configurations for the application.
- `.env`: Holds sensitive information for local development (e.g., access keys). This file is not checked into version control.
- `Dockerfile`: Defines the environment for building the application's Docker image.
- `spark-app.yaml`: The Kubernetes manifest for deploying the Spark application via the Spark Operator.
- `k8s-secret.yaml`: A template for creating Kubernetes Secrets to manage sensitive information in the production environment.
- `pyproject.toml`: Manages project dependencies using `uv`.
- `.gitignore`: Specifies files and directories to be ignored by Git.

## 3. How to Run

### Prerequisites

- Minikube
- kubectl
- Docker
- uv (Python package manager)
- An account with access to the company's JFrog container registry

### 3.1. Local Development Setup

1.  **Create a `.env` file:**

    Create a `.env` file in the root of the project and add your sensitive credentials:

    ```
    S3_ACCESS_KEY="your-access-key"
    S3_SECRET_KEY="your-secret-key"
    ```

2.  **Install dependencies and run the application:**

    ```bash
    uv sync
    python main.py
    ```

### 3.2. Production Deployment

#### 3.2.1. Build and Push the Docker Image

1.  **Build the Docker image:**

    ```bash
    docker build -t retest-report:latest .
    ```

2.  **Tag the image for the JFrog registry:**

    ```bash
    docker tag retest-report:latest your-jfrog-repo.jfrog.io/retest-report:latest
    ```

    *Note: Replace `your-jfrog-repo.jfrog.io` with the actual URL of your company's JFrog registry.*

3.  **Log in to the JFrog registry:**

    ```bash
    docker login your-jfrog-repo.jfrog.io
    ```

4.  **Push the image to the registry:**

    ```bash
    docker push your-jfrog-repo.jfrog.io/retest-report:latest
    ```

#### 3.2.2. Deploy the Application using Spark Operator

1.  **Create the Kubernetes Secret:**

    Before deploying the application, you need to create a Kubernetes Secret to store your sensitive credentials. Update the `k8s-secret.yaml` file with your base64-encoded credentials.

    To encode your credentials, you can use the following commands:

    ```bash
    echo -n 'your-access-key' | base64
    echo -n 'your-secret-key' | base64
    ```

    Apply the secret manifest:

    ```bash
    kubectl apply -f k8s-secret.yaml
    ```

2.  **Update the image path in `spark-app.yaml`:**

    Open the `spark-app.yaml` file and modify the `image` field to point to the image you just pushed to the JFrog registry.

    ```yaml
    ...
    spec:
      ...
      image: "your-jfrog-repo.jfrog.io/retest-report:latest"
      ...
    ```

3.  **Apply the application manifest:**

    ```bash
    kubectl apply -f spark-app.yaml
    ```

4.  **Monitor the application status:**

    You can check the status of the Spark application pods using the following command:

    ```bash
    kubectl get pods -l spark-app-selector=spark-retest-report
    ```

    To view the logs from the driver pod, you can use:

    ```bash
    kubectl logs <driver-pod-name>
    ```
