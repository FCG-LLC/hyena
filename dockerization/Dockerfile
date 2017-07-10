FROM portus.cs.int:5000/prod/cs-base
ARG destEnv
ARG hyena_version
ARG hyena_package
MAINTAINER lm@cs.com

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get install apt-transport-https -y --force-yes

RUN groupadd -g 10024 hyena
RUN adduser --no-create-home --system --shell /bin/false --gecos 'hyena dedicated user' --uid 10024 --gid 10024 --disabled-password hyena

RUN echo "deb http://10.12.1.225/public xenial $destEnv" >> /etc/apt/sources.list
RUN echo "Package: * \
Pin: release a=xenial \
Pin-Priority: 1600 \
" > /etc/apt/preferences

RUN apt-get update
RUN apt-get install -y nanomsg
RUN apt-get install -y ${hyena_package:-"hyena"}=${hyena_version:-"*"}

VOLUME "/tmp/hyena"

RUN chown hyena:hyena /tmp/hyena

USER hyena

CMD /usr/bin/hyena
