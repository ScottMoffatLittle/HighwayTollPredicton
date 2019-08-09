
### Kinetica Machine Learning BlackBox Container SDK Sample
# __Automated I-66 Highway Toll Forecasts__
### by Scott Little, Julian Jenkins, Saif Ahmed
### Copyright (c) 2019 Kinetica DB Inc.
#### For support: support@kinetica.com

![Kinetica Logo](https://kinetica.s3.amazonaws.com/icon_p2p.png)


### Background

Our model was trained on the full history of I-66 Toll data (mid 2018 - present), as well as weather data from the same period. It utilizes linear regression to forecast toll rates, and object detection algorithms running on VDOT (Virginia Department of Transportation) highway camera feeds to measure the speed and throughput of cars in order to augment the model's predicitons.

### Usage

Our model can be used stand-alone or within Kinetica Active Analytics Workbench. The model implements the Kinetica BlackBox SDK r7.0.5 to enable highly resilient operations for the toll forecasting.
For platform details, see the QuickStart at https://www.kinetica.com/tutorial/ml-powered-analytics/

