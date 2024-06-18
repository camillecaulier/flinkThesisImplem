location=$1

#scp ./src/main/python/setupScript.py ccaulier@access.grid5000.fr:$location/setupScript.py
#echo "setupScript.py transferred"
#scp .\data50.zip ccaulier@access.grid5000.fr:$location/data50.zip
#echo "data50.zip transferred"
#scp ./src/main/python/runPrometheusAndFlink.sh ccaulier@access.grid5000.fr:$location/runPrometheusAndFlink.sh
#echo "runPrometheusAndFlink.sh transferred"
#scp ./src/main/python/fakePrometheus.py ccaulier@access.grid5000.fr:$location/fakePrometheus.py
#echo "fakePrometheus.py transferred"
scp ./target/flinkImplemProject.jar ccaulier@access.grid5000.fr:$location/flinkImplemProjectTesting.jar
echo "flinkImpleProject.jar transferred"
#scp .\data_10_500000.zip ccaulier@access.grid5000.fr:$location/data_10_500000.zip
#echo "data_10_500000.zip transferred"