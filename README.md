docker run -d --restart=unless-stopped \
 --name rancher-local \
 --privileged \
 -p 80:80 \
 -p 443:443 \
 rancher/rancher:v2.13.0

docker logs rancher-local 2>&1 | grep "Bootstrap Password:"

GfnyIj8nVrOYFXcp

code ~/.kube/config

kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.32/deploy/local-path-storage.yaml

kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

kubectl get storageclass

helm upgrade --install kafka bitnami/kafka \
 --namespace kafka \
 --create-namespace \
 --version=34.4.3 \
 -f kafka/values.yaml

helm repo add kafbat https://ui.charts.kafbat.io
helm repo update

helm install kafka-ui kafbat/kafka-ui \
 --namespace kafka-ui \
 --create-namespace \
 --version=1.5.3 \
 --set image.repository=kafbat/kafka-ui \
 --set image.tag=v1.4.2 \
 --set envs.config.KAFKA_CLUSTERS_0_NAME=local-kafka \
 --set envs.config.KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka.kafka.svc.cluster.local:9092 \
 --set envs.config.KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=PLAINTEXT

kubectl port-forward svc/kafka-ui 8080:80 -n kafka-ui

kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.1/cert-manager.yaml

helm repo add flink-k8s-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0

helm install flink-kubernetes-operator flink-k8s-operator/flink-kubernetes-operator -n flink --create-namespace

kubectl delete pod freight-events-simulator -n kafka

kubectl run freight-events-simulator \
 -n kafka \
 --image=rhianlopes/freight-events-simulator:1.0.1 \
 --restart=Never \
 --env="KAFKA_BOOTSTRAP=kafka.kafka.svc.cluster.local:9092" \
 --env="TOPIC_FREIGHT_EVENTS=freight.events" \
 --env="TOPIC_TRACKING_EVENTS=tracking.events" \
 --env="EVENT_INTERVAL_SECONDS=1" \
 --env="LOG_LEVEL=INFO"

helm install freight-leads-app ./charts/flink-python-app \
 -n flink \
 --create-namespace \
 -f charts/flink-python-app/values.yaml \
 -f charts/flink-python-app/values-freight-leads.yaml

docker build -t rhianlopes/freight-events-simulator:1.0.0 .
docker push rhianlopes/freight-events-simulator:1.0.1

tava pensando algo como:

Explorando o Apache Flink (Parte 1): Construindo Playground com Rancher, Kafka, Kafka UI e Flink Kubernetes Operator
Explorando o Apache Flink (Parte 2): Construindo um Stateful Aggregator com PyFlink e Flink SQL
