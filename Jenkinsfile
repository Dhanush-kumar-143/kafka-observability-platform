// Declarative pipeline: lint -> test -> build -> Trivy scan -> push to ECR -> deploy to EKS
//
// Jenkins requirements:
//   * Agent with: docker, python3 (+venv), aws cli, kubectl, envsubst (gettext), trivy
//   * Credentials: "aws-credentials" (AWS Credentials plugin) with ECR + EKS access
//   * Pipeline parameters / env below set to match your account
pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  parameters {
    booleanParam(name: 'PUSH_AND_DEPLOY', defaultValue: false,
                 description: 'Push images to ECR and deploy to EKS (main branch builds)')
  }

  environment {
    AWS_REGION   = 'ap-south-1'
    ECR_REGISTRY = '123456789012.dkr.ecr.ap-south-1.amazonaws.com' // <-- change me
    EKS_CLUSTER  = 'kafka-obs-eks'
    IMAGE_TAG    = "${env.BUILD_NUMBER}-${env.GIT_COMMIT?.take(7) ?: 'local'}"
    APP_IMAGE    = "kafka-obs-app:${IMAGE_TAG}"
    KAFKA_IMAGE  = "kafka-obs-kafka:${IMAGE_TAG}"
  }

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Lint') {
      steps {
        sh '''
          python3 -m venv .venv
          . .venv/bin/activate
          pip install --quiet -r requirements-dev.txt
          flake8 app tests
        '''
        sh 'docker compose config --quiet'
      }
    }

    stage('Unit tests') {
      steps {
        sh '''
          . .venv/bin/activate
          pytest -q
        '''
      }
    }

    stage('Build images') {
      steps {
        sh 'docker build -t $APP_IMAGE .'
        sh 'docker build -t $KAFKA_IMAGE ./kafka'
      }
    }

    // Quality gate: the build fails if fixable HIGH/CRITICAL vulnerabilities are found
    stage('Trivy scan') {
      steps {
        sh '''
          trivy image --exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed --no-progress $APP_IMAGE
          trivy image --exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed --no-progress $KAFKA_IMAGE
        '''
      }
    }

    stage('Push to ECR') {
      when { expression { params.PUSH_AND_DEPLOY } }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-credentials']]) {
          sh '''
            aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY
            for repo in kafka-obs-app kafka-obs-kafka; do
              aws ecr describe-repositories --repository-names $repo --region $AWS_REGION >/dev/null 2>&1 \
                || aws ecr create-repository --repository-name $repo --region $AWS_REGION --image-scanning-configuration scanOnPush=true
            done
            docker tag $APP_IMAGE   $ECR_REGISTRY/kafka-obs-app:$IMAGE_TAG
            docker tag $KAFKA_IMAGE $ECR_REGISTRY/kafka-obs-kafka:$IMAGE_TAG
            docker push $ECR_REGISTRY/kafka-obs-app:$IMAGE_TAG
            docker push $ECR_REGISTRY/kafka-obs-kafka:$IMAGE_TAG
          '''
        }
      }
    }

    stage('Deploy to EKS') {
      when { expression { params.PUSH_AND_DEPLOY } }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-credentials']]) {
          sh '''
            aws eks update-kubeconfig --name $EKS_CLUSTER --region $AWS_REGION
            ./scripts/deploy-eks.sh
          '''
        }
      }
    }
  }

  post {
    always {
      sh 'docker image prune -f || true'
    }
    failure {
      echo 'Pipeline failed. Check the failing stage (lint, tests or Trivy scan).'
    }
  }
}
