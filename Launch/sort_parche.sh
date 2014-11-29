# Cambiamos el nombre del job
#$ -N testhadoop
# Indicamos el shell a usar:
#$ -S /bin/bash
# Indicamos las versiones a usar de hadoop (imprescindible):
#$ -v JAVA_HOME=/Soft/java/jdk1.6.0_30,HADOOP_HOME=/scratch/nas/2/martam/hadoop-0.20.203.0,HADOOP_CONF=/scratch/nas/2/martam/conf
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
# path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT

LOCAL_HD=/users/scratch/$USER/$JOB_NAME"_"$JOB_ID
NUM_REDS=16
### Copiamos los libros al sistema de ficheros HDFS de hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF fs -copyFromLocal "/scratch/nas/2/$USER/input_sort" $INPUT

$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

#path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT
# compress
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi
RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT/E4
mkdir -p $RESULT/E5

${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT

### Contamos las palabras, usando el ejemplo que viene con hadoop:

declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=0.5 [25]=0.25 [10]=0.10 [5]=0.05 [1]=0.01 )
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar sort -D sampling.estrategia=4 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} \
			$COMPRESS_OPT \
            -outKey org.apache.hadoop.io.Text \
            -outValue org.apache.hadoop.io.Text \
            -r ${NUM_REDS} \
			$INPUT $OUTPUT/E4/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/sort/E4/IT_$ITERATION/"
        while ! (rsync --partial -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/sort/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt) ; do sleep 1 ; done

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E4/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt

    done
done


declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=2 [25]=4 [10]=10 [5]=20 [1]=100 )
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar sort -D sampling.estrategia=5 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} \
		   $COMPRESS_OPT \
            -outKey org.apache.hadoop.io.Text \
            -outValue org.apache.hadoop.io.Text \
            -r ${NUM_REDS} \
		$INPUT $OUTPUT/E5/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E5/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/sort/E5/IT_$ITERATION/"
        while ! (rsync --partial -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/sort/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt) ; do sleep 1 ; done

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E5/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt

    done
done


# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 4 -P $RESULT/links/ http://localhost:50030

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

