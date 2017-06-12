FROM portus.cs.int:5000/prod/rust-base

USER root

RUN apt install -y wget

RUN echo "deb http://10.12.1.225/public trusty main" | tee -a /etc/apt/sources.list
RUN wget http://10.12.1.225/public/cs-repo.key -O - | apt-key add -

RUN apt-get update

RUN apt-get install -y nanomsg libnanomsg-dev

USER app
