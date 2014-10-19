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

# for prepare (per node) - 4G/node
DATASIZE=4000000000
NUM_MAPS=4

# for running (in total)
NUM_REDS=1

#  FI CONF

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
	-outFormat org.apache.hadoop.mapred.TextOutputFormat \
    $INPUT

$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

#path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_100 2 100

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_50 2 50

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_25 2 25

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_10 2 10

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_5 2 5

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount $INPUT $OUTPUT/E1/IT_1 2 1
### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT
mkdir -p $RESULT/maps
mkdir -p $RESULT/E1
 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT

${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_100 $RESULT/E1/E1_100.txt 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_50 $RESULT/E1/E1_50.txt 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_25 $RESULT/E1/E1_25.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_10 $RESULT/E1/E1_10.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_5 $RESULT/E1/E1_5.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_1 $RESULT/E1/E1_1.txt

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

${HADOOP_HOME}/bin/hadoop fs -rmr $INPUT_HDFS
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT

curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json

