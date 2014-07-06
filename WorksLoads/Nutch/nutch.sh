# Cambiamos el nombre del job
#$ -N testhadoop
# Indicamos el shell a usar:
#$ -S /bin/bash
# Indicamos las versiones a usar de hadoop (imprescindible):
#$ -v JAVA_HOME=/Soft/java/jdk1.6.0_30,HADOOP_HOME=/Soft/hadoop/0.20.203.0,HADOOP_CONF=/scratch/nas/2/martam/conf
# Indicamos que nos envie  un correo cuando empieze el trabajo y cuando acabe...
#$ -m bea
# ... a esta dirección de correo
#$ -M martam@ac.upc.edu

export CONF=/scratch/nas/2/martam/conf
export HADOOP_CONF_DIR=$CONF

### Definimos unos directorios de trabajo dentro del HDFS:
INPUT=$JOB_NAME"_"$JOB_ID"_IP"
OUTPUT=$JOB_NAME"_"$JOB_ID"_OP"

# //// hibench config
export COMPRESS_GLOBAL=0
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
# Lloc on esta descomprimit el hibench
export HIBENCH_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa"
# base dir HDFS
export DATA_HDFS=/$INPUT/HiBench
export NUTCH_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa/nutchindexing/nutch-1.2"
export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
NUTCH_INPUT="Input"
NUTCH_OUTPUT="Output"
NUTCH_BASE_HDFS=${DATA_HDFS}/Nutch

INPUT_HDFS=${NUTCH_BASE_HDFS}/${NUTCH_INPUT}
OUTPUT_HDFS=${NUTCH_BASE_HDFS}/${NUTCH_OUTPUT}

#PAGES=10000000
PAGES=50000
NUM_MAPS=8
NUM_REDS=4
# compress
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
fi

# generate data
OPTION="-t nutch \
        -b ${NUTCH_BASE_HDFS} \
        -n ${NUTCH_INPUT} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -o sequence"

$HADOOP_HOME/bin/hadoop jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT}

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPTS="-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPTS="-D mapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop fs -rmr $INPUT_HDFS/indexes

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`
export NUTCH_CONF_DIR=$HADOOP_CONF_DIR
${HADOOP_HOME}/bin/hadoop --config $CONF fs -lsr /
# run bench
$NUTCH_HOME/bin/nutch index $COMPRESS_OPTS $INPUT_HDFS/indexes $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*

$HADOOP_HOME/bin/hadoop fs -lsr /
### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$OUTPUT
mkdir $RESULT
mkdir $RESULT/maps
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $DATA_HDFS $RESULT
wget -O $RESULT/web_jobhistoryhome.html http://localhost:50030/jobhistoryhome.jsp
wget -O $RESULT/web_jobtracker.html http://localhost:50030/jobtracker.jsp
wget -O $RESULT/web_machines.html "http://localhost:50030/machines.jsp?type=active"

HADOOP_INTERNAL_JOB_ID=`grep -o 'job_[0-9]\+_[0-9]\+' $RESULT/web_jobtracker.html | head -n 1`
echo "El hadoop job id trobat es $HADOOP_INTERNAL_JOB_ID"
wget -O $RESULT/web_job.html "http://localhost:50030/jobdetails.jsp?jobid=$HADOOP_INTERNAL_JOB_ID"
wget -O $RESULT/web_map.html "http://localhost:50030/jobtasks.jsp?jobid=$HADOOP_INTERNAL_JOB_ID&type=map&pagenum=1"
wget -O $RESULT/web_reduce.html "http://localhost:50030/jobtasks.jsp?jobid=$HADOOP_INTERNAL_JOB_ID&type=reduce&pagenum=1"

HADOOP_MAP_TASKS=`grep -o 'taskdetails.jsp?tipid=task_[0-9]\+_[0-9]\+_m_[0-9]\+' $RESULT/web_map.html`
while read line
do
    MAP_TASK_ID=`echo $line | grep -o 'task_[0-9]\+_[0-9]\+_m_[0-9]\+'`
    echo "Hadoop map task id trobat: $MAP_TASK_ID"
    wget -O $RESULT/maps/$MAP_TASK_ID.html "http://localhost:50030/taskdetails.jsp?tipid=${MAP_TASK_ID}"
done <<< "$HADOOP_MAP_TASKS"

# //BORREM LES DADES DEL HDFS

${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $DATA_HDFS

curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json
