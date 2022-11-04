cd ~

# zip the data
zip -r ccc_crawler.zip "Dimensions_Data_Gathering/data"
zip -r logs.zip "Dimensions_Data_Gathering/logs"
zip -r config.zip "Dimensions_Data_Gathering/config"

# move to zip file
mv ccc_crawler.zip logs_config_full_data/
mv logs.zip logs_config_full_data/
mv config.zip logs_config_full_data/
