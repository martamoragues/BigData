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

# base dir HDFS
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*
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

INPUT=${PAGERANK_BASE_HDFS}/${PAGERANK_INPUT}
OUTPUT=${PAGERANK_BASE_HDFS}/${PAGERANK_OUTPUT}
### Copiamos los libros al sistema de ficheros HDFS de hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF fs -copyFromLocal "/scratch/nas/2/$USER/input-pagerank-light" $INPUT
# for prepare
PAGES=25
NUM_MAPS=16
NUM_REDS=16

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


$HADOOP_HOME/bin/hadoop --config $CONF fs -rmr ${INPUT}/edges/_*
$HADOOP_HOME/bin/hadoop --config $CONF fs -rmr ${INPUT}/vertices/_*
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
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT

RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT/E1
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT

for ITERATION in {1..1}
do
    for PERCENT in 100 50 25
    do
	OPTION="${COMPRESS_OPT} ${INPUT}/edges ${OUTPUT}/E1/IT_1/$PERCENT ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} nosym new"
    $HADOOP_HOME/bin/hadoop --config $CONF jar $HIBENCH_HOME/pagerank/pegasus-2.0.jar pegasus.PagerankNaive -D sampling.seed=2 -D sampling.estrategia=1 -D sampling.P=$PERCENT $OPTION
	        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/pr/E1/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/pr/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E1/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt
	done
done
#///fi pagerank run

# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 4 -P $RESULT/links/ http://localhost:50030

# //BORREM LES DADES DEL HDFS
# tancar totes les connexions ssh
ps -f -C ssh | grep "ssh -N -f" | awk '{print $2}' | xargs kill

${HADOOP_HOME}/bin/hadoop fs -rmr $INPUT
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT
curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json





