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
export MAHOUT_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa/common/mahout-distribution-0.9"
export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
# compression
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL


# paths
INPUT_HDFS=${DATA_HDFS}/KMeans/Input
OUTPUT_HDFS=${DATA_HDFS}/KMeans/Output
if [ $COMPRESS -eq 1 ]; then
    INPUT_HDFS=${INPUT_HDFS}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
fi
INPUT_SAMPLE=${INPUT_HDFS}/samples
INPUT_CLUSTER=${INPUT_HDFS}/cluster

# for prepare
NUM_OF_CLUSTERS=2
#NUM_OF_SAMPLES=20000000
NUM_OF_SAMPLES=5
#SAMPLES_PER_INPUTFILE=4000000
SAMPLES_PER_INPUTFILE=5
DIMENSIONS=2

# for running
MAX_ITERATION=5

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-compress true \
        -compressCodec $COMPRESS_CODEC \
        -compressType BLOCK "
else
    COMPRESS_OPT="-compress false"
fi

# paths check
$HADOOP_HOME/bin/hadoop dfs -rmr ${INPUT_HDFS}

# generate data
OPTION="-sampleDir ${INPUT_SAMPLE} -clusterDir ${INPUT_CLUSTER} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS}"
# export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath | tail -1`
echo $OPTION
echo $COMPRESS_OPT
# $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset ${COMPRESS_OPT} ${OPTION}
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-Dmapred.output.compress=true
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}

# copiar carpet output datagen a hadoop
$HADOOP_HOME/bin/hadoop fs -copyFromLocal "/scratch/nas/2/$USER/InputK/" $INPUT_HDFS
# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus ${INPUT_HDFS} | awk '{ print $2 }'`
OPTION="$COMPRESS_OPT -i ${INPUT_SAMPLE} -c ${INPUT_CLUSTER} -o ${OUTPUT_HDFS} -x ${MAX_ITERATION} -ow -cl -cd 0.5 -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -xm mapreduce"

# run bench
${MAHOUT_HOME}/bin/mahout kmeans  ${OPTION}

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
curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json

### Esperar aqui fins que existeixi el fitxer
while [ ! -f /scratch/nas/2/martam/control_execution/finish ]
do
    sleep 10
done


# //BORREM LES DADES DEL HDFS

${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $DATA_HDFS

