package testutils

import (
	"context"
	"log"
	"testing"
	"time"
	"net"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"

)

// Creates a Kafka container in KRaft mode
func CreateKafkaWithKRaftContainer(dockerClient *client.Client, kafkaAddr net.Addr) (id string, err error) {
	ctx := context.Background()

	config := &container.Config{
		Image: "apache/kafka:3.7.0",
		ExposedPorts: nat.PortSet{
			"9092": struct{}{},
		},
		Tty: false,
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"9092": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "9092",
				},
			},
		},
	}

	resp, err := dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, "kafka")
	if err != nil {
		panic(err)
	}

	if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	log.Println("WAITING FOR KAFKA CONTAINER TO START...")

	if err = waitKafkaIsUp(kafkaAddr); err != nil {
		panic(err)
	}

	return resp.ID, nil
}

// Shutdowns the container gracefully or fails the test in case of the error
func ContainerGracefulShutdown(t *testing.T, dockerClient *client.Client, containerId string) {
	if err := gracefulShutdown(dockerClient, containerId); err != nil {
		t.Fatalf("could not remove container %v, consider deleting it manually!", err)
	}
}

func gracefulShutdown(dockerClient *client.Client, containerId string) error {
	log.Printf("Removing container %s \n", containerId)
	inspection, err := dockerClient.ContainerInspect(context.Background(), containerId)
	if err != nil {
		log.Printf("could not inspect container %v, consider deleting it manually!", err)
		return err
	}

	if !inspection.State.Running {
		log.Printf("Container %s is not running, proceeding to remove it.\n", containerId)
		return removeContainer(dockerClient, containerId)
	}

	log.Printf("Stopping container %s \n", containerId)
	timeout := 5
	if err = dockerClient.ContainerStop(context.Background(), containerId, container.StopOptions{Timeout: &timeout}); err != nil {
		log.Printf("Could not stop container %v, consider deleting it manually!", err)
		return err
	}

	return removeContainer(dockerClient, containerId)
}

func removeContainer(dockerClient *client.Client, containerId string) error {
	log.Printf("Removing container %s \n", containerId)
	if err := dockerClient.ContainerRemove(context.Background(), containerId, container.RemoveOptions{Force: true}); err != nil {
		log.Printf("Could not remove container %v, consider deleting it manually!", err)
		return err
	}
	return nil
}

func waitKafkaIsUp(kafkaAddr net.Addr) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	kafkaClient := &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	timerDuration := time.Duration(1) * time.Second
	timer := time.NewTimer(timerDuration)

	for {
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		case <-timer.C:
			log.Println("WAITING FOR KAFKA TO BE READY...")
			resp, err := kafkaClient.Metadata(ctx, &kafka.MetadataRequest{ // for some reason Heartbeat request is not enough: even if it's successful, the client may fail to create a topic
				Addr: kafkaAddr,
			})
			if resp == nil || err != nil {
				timer.Reset(timerDuration)
				continue
			}
			cancel()
			return nil
		}
	}
}
