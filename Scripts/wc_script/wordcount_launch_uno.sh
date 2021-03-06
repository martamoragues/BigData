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

### Copiamos los libros al sistema de ficheros HDFS de hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF fs -copyFromLocal "/scratch/nas/2/$USER/marta" $INPUT

$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

#path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT

RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT/E1
mkdir -p $RESULT/E2
mkdir -p $RESULT/E3
mkdir -p $RESULT/E4
mkdir -p $RESULT/E5

${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT

for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar wordcount -D sampling.seed=2 -D sampling.estrategia=1 -D sampling.P=$PERCENT $INPUT $OUTPUT/E1/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E1/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/wc/E1/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/wc/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E1/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E1/IT_$ITERATION/E1_$(printf %03d $PERCENT).txt

    done
done



### Contamos las palabras, usando el ejemplo que viene con hadoop:
declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=2 [25]=4 [10]=10 [5]=20 [1]=100 )
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar wordcount -D sampling.seed=2 -D sampling.estrategia=2 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} $INPUT $OUTPUT/E2/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E2/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E2/IT_$ITERATION/E2_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/wc/E2/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E2/IT_$ITERATION/E2_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/wc/E2/IT_$ITERATION/E2_$(printf %03d $PERCENT).txt

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E2/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E2/IT_$ITERATION/E2_$(printf %03d $PERCENT).txt

    done
done

declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=0.5 [25]=0.25 [10]=0.10 [5]=0.05 [1]=0.01 )
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar wordcount -D sampling.all.file.size=3544058682 -D sampling.estrategia=3 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} $INPUT $OUTPUT/E3/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E3/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E3/IT_$ITERATION/E3_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/wc/E3/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E3/IT_$ITERATION/E3_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/wc/E3/IT_$ITERATION/E3_$(printf %03d $PERCENT).txt

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from HDFS"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -rmr $OUTPUT/E3/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Removing all data of E${EXECUTION} IT_$ITERATION P$PERCENT from local hd"
        rm -rf $LOCAL_HD/E3/IT_$ITERATION/E3_$(printf %03d $PERCENT).txt

    done
done

declare -a TRADUCCIO_PERCENT=( [100]=1 [50]=0.5 [25]=0.25 [10]=0.10 [5]=0.05 [1]=0.01 )
for ITERATION in {1..5}
do
    for PERCENT in 100 50 25 10 5 1
    do
        echo "Executing iteration $ITERATION with percent $PERCENT"
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar wordcount -D sampling.estrategia=4 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} $INPUT $OUTPUT/E4/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/wc/E4/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/wc/E4/IT_$ITERATION/E4_$(printf %03d $PERCENT).txt

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
        ${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/martam/hadoop-0.20.203.0/hadoop-examples-0.20.203.0.jar wordcount -D sampling.estrategia=5 -D sampling.P=${TRADUCCIO_PERCENT[$PERCENT]} $INPUT $OUTPUT/E5/IT_$ITERATION/$(printf %03d $PERCENT)

        echo "Copying results of iteration $ITERATION with percent $PERCENT to local hd"
        ${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E5/IT_$ITERATION/$(printf %03d $PERCENT) $LOCAL_HD/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt

        echo "Copying form local hd of iteration $ITERATION with percent $PERCENT to remote pc"
        ssh -N -f -L 12025:127.0.0.1:12024 -i /scratch/nas/2/martam/ssh/id_dsa martam@172.18.3.3
        ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa jcugat@127.0.0.1 "mkdir -p /media/jcugat/marta/core/wc/E5/IT_$ITERATION/"
        rsync -az -e "ssh -p 12025 -oStrictHostKeyChecking=no -i /scratch/nas/2/martam/ssh/id_dsa" $LOCAL_HD/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt jcugat@127.0.0.1:/media/jcugat/marta/core/wc/E5/IT_$ITERATION/E5_$(printf %03d $PERCENT).txt

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

