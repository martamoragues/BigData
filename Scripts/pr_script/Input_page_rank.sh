# Cambiamos el nombre del job
#$ -N testhadoop
# Indicamos el shell a usar:
#$ -S /bin/bash
# Indicamos las versiones a usar de hadoop (imprescindible):
#$ -v JAVA_HOME=/usr,HADOOP_HOME=/scratch/nas/2/martam/hadoop-0.20.203.0,HADOOP_CONF=/scratch/nas/2/martam/conf
# Indicamos que nos envie  un correo cuando empieze el trabajo y cuando acabe...
#$ -m bea
# ... a esta dirección de correo
#$ -M martam@ac.upc.edu

export CONF=/scratch/nas/2/martam/conf


### Definimos unos directorios de trabajo dentro del HDFS:
INPUT=$JOB_NAME"_"$JOB_ID"_IP"
OUTPUT=$JOB_NAME"_"$JOB_ID"_OP"


echo "ini hibench config"
# //// hibench config

# Lloc on esta descomprimit el hibench
export HIBENCH_HOME="/scratch/nas/2/martam/Hibench/HiBench-master/"

if [ -z "$DATATOOLS" ]; then
    export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
fi

# base dir HDFS
export DATA_HDFS=$INPUT/HiBench

# swith on/off compression: 0-off, 1-on
export COMPRESS_GLOBAL=0
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec

# // fi hibench config

echo "fi hibench config"
echo "ini pagerank config"
# /////pagerank configure

# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
PAGERANK_INPUT="Input"
PAGERANK_OUTPUT="Output"
PAGERANK_BASE_HDFS=${DATA_HDFS}/Pagerank

if [ $COMPRESS -eq 1 ]; then
    PAGERANK_INPUT=${PAGERANK_INPUT}-comp
    PAGERANK_OUTPUT=${PAGERANK_OUTPUT}-comp
fi

INPUT_HDFS=${PAGERANK_BASE_HDFS}/${PAGERANK_INPUT}
OUTPUT_HDFS=${PAGERANK_BASE_HDFS}/${PAGERANK_OUTPUT}

# for prepare
PAGES=500000
NUM_MAPS=1
NUM_REDS=1

# for running
NUM_ITERATIONS=3
BLOCK=0
BLOCK_WIDTH=1

# ///fi pagerank configure

echo "fi pagerank configure"
echo "ini prepare pagerank"
# ////pagerank prepare

# compress
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
fi

# generate data
#DELIMITER=\t
OPTION="-t pagerank \
	-b ${PAGERANK_BASE_HDFS} \
	-n ${PAGERANK_INPUT} \
	-m ${NUM_MAPS} \
	-r ${NUM_REDS} \
	-p ${PAGES} \
	-o text"
echo $OPTION
echo "options:"
echo $COMPRESS_OPT
echo "compress"
echo $CONF
echo "conf"
echo $DATATOOLS
echo "datatools"

$HADOOP_HOME/bin/hadoop --config $CONF jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT}

$HADOOP_HOME/bin/hadoop --config $CONF fs -rmr ${INPUT_HDFS}/edges/_*
$HADOOP_HOME/bin/hadoop --config $CONF fs -rmr ${INPUT_HDFS}/vertices/_*
# /// fi pagerank prepare
echo "fi prepare pagerank"
echo "ini run pagerank"
#/// inici pagerank run


# compress check
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
${HADOOP_HOME}/bin/hadoop --config $CONF fs -lsr /
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT_HDFS

#///fi pagerank run

### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$OUTPUT
mkdir $RESULT
mkdir $RESULT/E1
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $PAGERANK_BASE_HDFS $RESULT

# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 4 -P $RESULT/links/ http://localhost:50030

# //BORREM LES DADES DEL HDFS

${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $DATA_HDFS

curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json





