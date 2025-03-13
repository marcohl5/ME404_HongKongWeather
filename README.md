# Hong Kong Weather Analysis

### Project Background
This analysis formed a part of the coursework for a 4th year Mechanical Engineering module - Energy Systems Modelling. The coursework specified the modelling of an energy system, assessing its need for heating and/or cooling throughout the year.

The selected energy system was a high-rise apartment in Hong Kong, located within [Island Harbour View](https://en.wikipedia.org/wiki/Island_Harbourview). Hong Kong's subtropical climate is characterised by hot, humid summers and mild winters.

![image](https://github.com/user-attachments/assets/69ec3c32-18d7-476e-803d-64d09aa91abe)


Analysing weather variables is essential, as their impact on heating and cooling energy consumption is highly climate-dependent. 


### Data Source

The data was gathered from the [Community Weather Information Network](https://cowin.hku.hk/english/blog.html) of Hong Kong. 

The data used in the analysis was from [this](https://cowin.hku.hk/english/site-6087.html) specific weather station. 
This collection point was chosen because it had complete data for all of 2023 and is located within 1 km of the selected energy system, enhancing the accuracy and relevance of the analysis.

The 2023 dataset was downloaded using the link below: 

[Download Link](https://cowin.hku.hk/english/downloadbulk.html)


### Data Preprocessing
The data was preprocessed using [data-processing.py](https://github.com/marcohl5/ME404_HongKongWeather/blob/main/data-processing.py).
* The weather data was provided in separate csv files for each weather variable, combined into a dataframe.
* Only the weather data for the selected weather station was kept (Station ID: 6087).
* The datetime column provided was split into a date (YYYY-MM-DD) and time column (HH:MM:SS).


### Data Analysis
The data analysis was conducted in [eda.ipynb](https://github.com/marcohl5/ME404_HongKongWeather/blob/main/eda.ipynb).

The notebook can also be viewed [here](https://nbviewer.org/github/marcohl5/ME404_HongKongWeather/blob/main/eda.ipynb).


### Pre/Postprocessing for the Energy System Model
Minor preprocessing and postprocessing steps for the model was conducted in [pre-postprocessing-model.ipynb](https://github.com/marcohl5/ME404_HongKongWeather/blob/main/pre-postprocessing-model.ipynb).


