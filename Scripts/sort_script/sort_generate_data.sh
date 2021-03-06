# Cambiamos el nombre del job
#$ -N testhadoop
# Indicamos el shell a usar:
#$ -S /bin/bash
# Indicamos las versiones a usar de hadoop (imprescindible):
#$ -v JAVA_HOME=/usr,HADOOP_HOME=/Soft/hadoop/0.20.203.0,HADOOP_CONF=/scratch/nas/2/martam/conf
# Indicamos que nos envie  un correo cuando empieze el trabajo y cuando acabe...
#$ -m bea
# ... a esta dirección de correo
#$ -M martam@ac.upc.edu

export CONF=/scratch/nas/2/martam/conf
# swith on/off compression: 0-off, 1-on
export COMPRESS_GLOBAL=0
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec

### Definimos unos directorios de trabajo dentro del HDFS:
INPUT=$JOB_NAME"_"$JOB_ID"_IP"
OUTPUT=$JOB_NAME"_"$JOB_ID"_OP"

# CONF
# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# for prepare (per node) - 24G/node
#DATASIZE=24000000000
DATASIZE=3544058682
NUM_MAPS=1

# for running (in total)
NUM_REDS=1

#  FI CONF

echo "========== preparing sort data=========="

# path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.output.compression.codec=$COMPRESS_CODEC \
    -D mapred.output.compression.type=BLOCK "
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi

# generate data
$HADOOP_HOME/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.RandomTextWriter \
    -D test.randomtextwrite.bytes_per_map=$((${DATASIZE} / ${NUM_MAPS})) \
    -D test.randomtextwrite.maps_per_host=${NUM_MAPS} \
	-D test.randomtextwrite.total_bytes=${DATASIZE} \
    $COMPRESS_OPT \
    $INPUT


$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

## RUN
echo "========== running sort bench =========="

# compress
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi

#path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop --config $CONF fs -dus $INPUT | awk '{ print $2 }'`


# run bench



#///fi pagerank run


### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT
mkdir -p $RESULT/E1

${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT


# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 1 -P $RESULT/links/ http://localhost:50030
# //BORREM LES DADES DEL HDFS

${HADOOP_HOME}/bin/hadoop fs -rmr $INPUT
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT

curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json




