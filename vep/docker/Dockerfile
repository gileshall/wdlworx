ARG VEP_VERSION
FROM ensemblorg/ensembl-vep:${VEP_VERSION}

ARG VEP_VERSION
ARG VEP_GENOME_ORGANISM
ARG VEP_GENOME_VERSION
ENV VEP_VERSION="${VEP_VERSION}"
ENV VEP_GENOME_ORGANISM="${VEP_GENOME_ORGANISM}"
ENV VEP_GENOME_VERSION="${VEP_GENOME_VERSION}"

#RUN INSTALL.pl -a cf -s ${VEP_GENOME_ORGANISM} -y ${VEP_GENOME_VERSION}
COPY ${VEP_GENOME_ORGANISM} ./${VEP_GENOME_ORGANISM}
