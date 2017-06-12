FROM portus.cs.int:5000/prod/cs-base
ARG destEnv
ARG hyena_version
MAINTAINER lm@cs.com

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get install apt-transport-https -y --force-yes

# for now collector user has to run with id == topdis
# because of mounts, ipc, permissions and stuff
# and yes, I know that this is ugly

RUN groupadd -g 10024 hyena
RUN adduser --no-create-home --system --shell /bin/false --gecos 'hyena dedicated user' --uid 10024 --gid 10024 --disabled-password hyena

RUN apt-get update
RUN apt-get install -y nanomsg=*$destEnv
RUN apt-get install -y hyena=${hyena_version:-"*"}

CMD /usr/bin/hyena
