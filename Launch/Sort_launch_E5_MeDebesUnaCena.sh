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
DATASIZE=4000000000
NUM_MAPS=16

# for running (in total)
NUM_REDS=16

#  FI CONF

LOCAL_HD=/users/scratch/$USER/$JOB_NAME"_"$JOB_ID

echo "========== preparing sort data=========="

# path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT

# generate data
${HADOOP_HOME}/bin/hadoop --config $CONF fs -copyFromLocal "/scratch/nas/2/$USER/input_sort" $INPUT

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.output.compression.codec=$COMPRESS_CODEC \
    -D mapred.output.compression.type=BLOCK "
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi

$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

# copy generated input data
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $LOCAL_HD/INPUT


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

declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=2 [25]=4 [10]=10 [5]=20 [1]=100 )

EXECUTION="5"
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        CURRENT_OUTPUT="E${EXECUTION}/IT_$ITERATION/E${EXECUTION}_$(printf %03d $PERCENT)"
        echo "Executing E${EXECUTION} IT_$ITERATION P$PERCENT"
        $HADOOP_HOME/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.Sort_E${EXECUTION} \
            $COMPRESS_OPT \
            -outKey org.apache.hadoop.io.Text \
            -outValue org.apache.hadoop.io.Text \
            -r ${NUM_REDS} \
            $INPUT $OUTPUT/$CURRENT_OUTPUT ${TRADUCCIO_PERCENT[$PERCENT]}

        echo "Copying all data of E${EXECUTION} IT_$ITERATION P$PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/$CURRENT_OUTPUT $LOCAL_HD/$CURRENT_OUTPUT.txt

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/$CURRENT_OUTPUT
    done
done


# post-running


RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT


# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 0 -P $RESULT/links/ http://localhost:50030
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

