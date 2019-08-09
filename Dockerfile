# Copyright (c) 2019 Kinetica DB Inc.
#
# Kinetica Machine Learning
# Kinetica Machine Learning BlackBox Container SDK + Trivial Sample
#
# for support, contact Saif Ahmed (support@kinetica.com)
#

FROM python:3.6
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils

# These utilities are only for debugging
# These can safely be removed in PROD settings, if desired
RUN apt-get install -y git htop wget nano

RUN mkdir -p /opt/gpudb/kml/bbx
WORKDIR "/opt/gpudb/kml/bbx"

# Install Required Libraries and Dependencies
ADD requirements.txt  ./
RUN pip install -r requirements.txt

# Add Kinetica BlackBox SDK (currently v7.0.5b)
ADD bb_runner.sh ./
ADD sdk ./sdk

ADD make_inference.py ./
ADD get_cur_data.py ./
ADD toll_model.sav ./
ADD toll_encoder.sav ./

RUN ["chmod", "+x",  "bb_runner.sh"]

CMD ["nohup /bin/bash -c "python update_data.py &" && sleep 4"]
ENTRYPOINT ["/opt/gpudb/kml/bbx/bb_runner.sh"]
