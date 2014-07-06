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


echo "ini hibench config"
# //// hibench config
export COMPRESS_GLOBAL=0
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
# Lloc on esta descomprimit el hibench
export HIBENCH_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa"
# base dir HDFS
export DATA_HDFS=/$INPUT/HiBench
export MAHOUT_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa/common/mahout-distribution-0.7"
export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
# compression
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
BAYES_INPUT="Input"
BAYES_OUTPUT="Output"
BAYES_BASE_HDFS=${DATA_HDFS}/Bayes

if [ $COMPRESS -eq 1 ]; then
    BAYES_INPUT=${BAYES_INPUT}-comp
    BAYES_OUTPUT=${BAYES_OUTPUT}-comp
fi
INPUT_HDFS=${BAYES_BASE_HDFS}/${BAYES_INPUT}
OUTPUT_HDFS=${BAYES_BASE_HDFS}/${BAYES_OUTPUT}

# for prepare
#PAGES=100000
PAGES=25000
CLASSES=50
NUM_MAPS=2
NUM_REDS=2

# bench parameters
NGRAMS=3

# compress check
if [ ${COMPRESS} -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
fi

# generate data
OPTION="-t bayes \
        -b ${BAYES_BASE_HDFS} \
        -n ${BAYES_INPUT} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -class ${CLASSES} \
        -o sequence"
echo "option   $OPTION"
echo "compres $COMPRESS_OPT"
# $HADOOP_HOME/bin/hadoop --config $CONF jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT}
# ${HADOOP_HOME}/bin/hadoop --config $CONF fs -lsr /
# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-Dmapred.output.compress=true
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
echo "path check"
echo "path check" 1>&2
${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr ${OUTPUT_HDFS}
# pre-running
echo "size check"
echo "size check" 1>&2

$HADOOP_HOME/bin/hadoop fs -copyFromLocal "/scratch/nas/2/$USER/inputBayes/" $INPUT_HDFS

SIZE=`$HADOOP_HOME/bin/hadoop --config $CONF fs -dus ${INPUT_HDFS} | awk '{ print $2 }'`
$HADOOP_HOME/bin/hadoop --config $CONF fs -dus ${INPUT_HDFS}
echo $SIZE
echo $SIZE 1>&2
# run bench
echo "mohout primer"
echo "mohout primer" 1>&2
$MAHOUT_HOME/bin/mahout seq2sparse \
        $COMPRESS_OPT -i ${INPUT_HDFS} -o ${OUTPUT_HDFS}/vectors  -lnorm -nv  -wt tfidf -ng ${NGRAMS}
echo "mahout segon"
echo "mahout segon" 1>&2
$MAHOUT_HOME/bin/mahout trainnb \
        $COMPRESS_OPT -i ${OUTPUT_HDFS}/vectors/tfidf-vectors -el -o ${OUTPUT_HDFS}/model -li ${OUTPUT_HDFS}/labelindex  -ow --tempDir ${OUTPUT_HDFS}/temp
echo "fi run pagerank"

### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$OUTPUT
mkdir $RESULT
mkdir $RESULT/maps
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $BAYES_BASE_HDFS $RESULT
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

### Notificacio
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
