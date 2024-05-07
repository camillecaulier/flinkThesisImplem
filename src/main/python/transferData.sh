location=$1

scp ./src/main/python/setupScript.py ccaulier@access.grid5000.fr:$location/setupScript.py
scp .\data50.zip ccaulier@access.grid5000.fr:$location/data50.zip
scp ./src/main/python/runPrometheusAndFlink.sh ccaulier@access.grid5000.fr:$location/runPrometheusAndFlink.sh
scp ./src/main/python/fakePrometheus.py ccaulier@access.grid5000.fr:$location/fakePrometheus.py