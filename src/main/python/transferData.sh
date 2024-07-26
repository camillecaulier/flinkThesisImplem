location=$1

scp ./src/main/python/setupScript.py ccaulier@access.grid5000.fr:$location/setupScript.py
echo "setupScript.py transferred"
scp ./src/main/python/runPrometheusAndFlink.sh ccaulier@access.grid5000.fr:$location/runPrometheusAndFlink.sh
echo "runPrometheusAndFlink.sh transferred"
scp ./src/main/python/fakePrometheus.py ccaulier@access.grid5000.fr:$location/fakePrometheus.py
echo "fakePrometheus.py transferred"
scp ./target/flinkImplemProject.jar ccaulier@access.grid5000.fr:$location/flinkImplemProject.jar
echo "flinkImpleProject.jar transferred"
#scp .\data_10_500000.zip ccaulier@access.grid5000.fr:$location/data_10_500000.zip
#echo "data_10_500000.zip transferred"

scp ./param_100000_600.csv ccaulier@access.grid5000.fr:$location/param_100000_600.csv
echo "param_100000_600.csv transferred"
scp ./param_100000_300.csv ccaulier@access.grid5000.fr:$location/param_100000_300.csv
echo "param_100000_300.csv transferred"
scp ./param_400000_75.csv ccaulier@access.grid5000.fr:$location/param_400000_75.csv
echo "param_400000_75.csv transferred"
scp ./param_100000_1000.csv ccaulier@access.grid5000.fr:$location/param_100000_1000.csv
echo "param_100000_1000.csv transferred"
scp ./param_100000_1200.csv ccaulier@access.grid5000.fr:$location/param_100000_1200.csv
echo "param_100000_1200.csv transferred"
scp ./param_400000_300.csv ccaulier@access.grid5000.fr:$location/param_400000_300.csv
echo "param_400000_300.csv transferred"
scp ./param_100000_360.csv ccaulier@access.grid5000.fr:$location/param_100000_360.csv
echo "param_100000_360.csv transferred"
scp ./param_400000_90.csv ccaulier@access.grid5000.fr:$location/param_400000_90.csv
echo "param_400000_90.csv transferred"


