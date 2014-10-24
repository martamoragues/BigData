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
# path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT

### Copiamos los libros al sistema de ficheros HDFS de hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF fs -copyFromLocal "/scratch/nas/2/$USER/marta" $INPUT

$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $INPUT/_*

#path check
$HADOOP_HOME/bin/hadoop --config $CONF dfs -rmr $OUTPUT

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_100 1

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_50 0.5

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_25 0.25

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_10 0.10

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_5 0.05

### Contamos las palabras, usando el ejemplo que viene con hadoop:
${HADOOP_HOME}/bin/hadoop --config $CONF jar /scratch/nas/2/$USER/wc.jar org.apache.hadoop.examples.WordCount_E4 $INPUT $OUTPUT/E4/IT_1 0.01
### Copiamos los datos del disco de hadoop HDFS a nuestra cuenta en el NAS:
RESULT=/scratch/nas/2/$USER/$JOB_NAME"_"$JOB_ID
mkdir -p $RESULT
mkdir -p $RESULT/E4
 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -get $INPUT $RESULT

${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_100 $RESULT/E4/E4_100.txt 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_50 $RESULT/E4/E4_50.txt 
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_25 $RESULT/E4/E4_25.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_10 $RESULT/E4/E4_10.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_5 $RESULT/E4/E4_5.txt
${HADOOP_HOME}/bin/hadoop --config $CONF fs -getmerge $OUTPUT/E4/IT_1 $RESULT/E4/E4_1.txt

# Descarrega tota la web
wget -q -r -k -p -nH --adjust-extension --exclude-directories=/logs/ -l 0 -P $RESULT/links/ http://localhost:50030
${HADOOP_HOME}/bin/hadoop fs -rmr $INPUT
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT

curl -s \
  -F "token=a4fSa7UW8vmxijUg6udU1cwUtM3tB7" \
  -F "user=OO4JgmIzwWBU44nigYskODlS8QUAs7" \
  -F "device=iPhone5" \
  -F "title=Hadoop" \
  -F "message=Acabat: $JOB_ID :)" \
  https://api.pushover.net/1/messages.json

