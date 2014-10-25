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


### Definimos unos directorios de trabajo dentro del HDFS:
INPUT=$JOB_NAME"_"$JOB_ID"_IP"
OUTPUT=$JOB_NAME"_"$JOB_ID"_OP"


echo "ini hibench config"
# //// hibench config

# Lloc on esta descomprimit el hibench
export HIBENCH_HOME="/scratch/nas/2/martam/intel-hadoop-HiBench-4aa2ffa"

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
DATASIZE=293000
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
PAGES=500
NUM_MAPS=8
NUM_REDS=4

# for running
NUM_ITERATIONS=3
BLOCK=0
BLOCK_WIDTH=16

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

if [ $BLOCK -eq 0 ]
then
    OPTION="${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} nosym new 0.5"
else
    OPTION="${COMPRESS_OPT} ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} ${BLOCK_WIDTH}"
fi

# $HADOOP_HOME/bin/hadoop --config $CONF fs -mkdir $OUTPUT_HDFS
# ${HADOOP_HOME}/bin/hadoop --config $CONF fs -lsr /

# run bench
if [ $BLOCK -eq 0 ]
then
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.PagerankNaive $OPTION
else
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.PagerankInitVector ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${PAGES} ${NUM_REDS}
    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_input

    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_iv_block
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${OUTPUT_HDFS}/pr_iv_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} s makesym
    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_initvector

    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_edge_colnorm
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.PagerankPrep ${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS}/pr_edge_colnorm ${NUM_REDS} makesym

    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_edge_block
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_edge_colnorm ${OUTPUT_HDFS}/pr_edge_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} null nosym
    $HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr ${OUTPUT_HDFS}/pr_edge_colnorm

    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.PagerankBlock ${OPTION}
fi

#///fi pagerank run
echo "fi run pagerank"

### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$OUTPUT
mkdir $RESULT
mkdir $RESULT/maps
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $PAGERANK_BASE_HDFS $RESULT
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




