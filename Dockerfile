FROM portus.cs.int:5000/prod/rust-snmp-base
ARG destEnv
ARG git_commit
ARG git_source
ARG jenkins_jobid

LABEL maintainer="Jacek Całusiński <forger@collective-sense.com>" \
	collective.git.commit="${git_commit}" \
	collective.git.source="${git_source}" \
	collective.jenkins.jobid="${jenkins_jobid}"

USER root

RUN echo "deb http://10.12.1.225/public trusty main" | tee -a /etc/apt/sources.list

RUN echo "deb http://10.12.1.225/public xenial $destEnv" >> /etc/apt/sources.list
RUN echo "Package: * \
Pin: release a=xenial \
Pin-Priority: 1600 \
" > /etc/apt/preferences

RUN wget http://10.12.1.225/public/cs-repo.key -O - | apt-key add -

RUN apt-get update

RUN apt-get install -y nanomsg nanomsg-dev

USER app
