pipeline {
    agent any

    environment {
        PYTHON = 'python3'
        VENV_DIR = '.venv'
    }

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/nrk16p/etl_terminus.git'
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''#!/bin/bash
                if [ ! -d "$VENV_DIR" ]; then
                    $PYTHON -m venv $VENV_DIR
                fi
                . $VENV_DIR/bin/activate
                pip install --upgrade pip
                pip install -r requirements.txt
                '''
            }
        }

        stage('Run ETL script') {
            steps {
                sh '''#!/bin/bash
                . $VENV_DIR/bin/activate
                python main.py
                '''
            }
        }
    }

    post {
        success {
            echo '✅ ETL Terminus completed successfully!'
        }
        failure {
            echo '❌ ETL Terminus failed.'
        }
    }
}