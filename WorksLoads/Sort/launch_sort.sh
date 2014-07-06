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

# base dir HDFS
export DATA_HDFS=$INPUT/HiBench

# paths
INPUT_HDFS=${DATA_HDFS}/Sort/Input
OUTPUT_HDFS=${DATA_HDFS}/Sort/Output

if [ $COMPRESS -eq 1 ]; then
    INPUT_HDFS=${INPUT_HDFS}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
fi

# for prepare (per node) - 24G/node
#DATASIZE=24000000000
DATASIZE=240000
NUM_MAPS=1

# for running (in total)
NUM_REDS=1

#  FI CONF


echo "========== preparing sort data=========="

# path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT_HDFS

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
    $INPUT_HDFS
echo "final sortida $?"


$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT_HDFS/_*

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
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT_HDFS

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop --config $CONF fs -dus $INPUT_HDFS | awk '{ print $2 }'`


# run bench

$HADOOP_HOME/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.Sort \
    $COMPRESS_OPT \
    -outKey org.apache.hadoop.io.Text \
    -outValue org.apache.hadoop.io.Text \
    -r ${NUM_REDS} \
    $INPUT_HDFS $OUTPUT_HDFS

# post-running

#///fi pagerank run
echo "fi run pagerank"


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



