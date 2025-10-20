pipeline {
    agent any

    environment {
        PYTHON = 'python3'
        VENV_DIR = '.venv'
    }

    triggers {
        cron('H 7 * * *')  // run every day at 7am
    }

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/nrk16p/etl_terminus.git'
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''
                if [ ! -d "$VENV_DIR" ]; then
                    $PYTHON -m venv $VENV_DIR
                fi
                source $VENV_DIR/bin/activate
                pip install --upgrade pip
                pip install -r requirements.txt
                '''
            }
        }

        stage('Run ETL script') {
            steps {
                sh '''
                source $VENV_DIR/bin/activate
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
