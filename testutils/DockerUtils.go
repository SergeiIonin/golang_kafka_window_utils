package testutils

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
)

func CleanupAndGracefulShutdown(t *testing.T, dockerClient *client.Client, containerId string) {
	if err := GracefulShutdown(dockerClient, containerId); err != nil {
		t.Fatalf("could not remove container %v, consider deleting it manually!", err)
	}
}

func GracefulShutdown(dockerClient *client.Client, containerId string) error {
	log.Printf("Removing container %s \n", containerId)
	inspection, err := dockerClient.ContainerInspect(context.Background(), containerId)
	if err != nil {
		log.Printf("could not inspect container %v, consider deleting it manually!", err)
		return err
	}

	if !inspection.State.Running {
		return nil
	}

	if err = dockerClient.ContainerRemove(context.Background(), containerId, container.RemoveOptions{Force: true}); err != nil {
		log.Printf("could not remove container %v, consider deleting it manually!", err)
		return err
	}
	return nil
}

func waitKafkaIsUp() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	kafkaClient := &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	for {
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		default:
			log.Println("WAITING FOR KAFKA TO BE READY...")
			// for some reason Heartbeat request is not enough: even if it's successful,
			// the client may fail to creata topic
			resp, err := kafkaClient.Metadata(ctx, &kafka.MetadataRequest{
				Addr: kafkaAddr,
			})
			if resp == nil || err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			cancel()
			return nil
		}
	}
}

func CreateKafkaWithKRaftContainer(dockerClient *client.Client) (id string, err error) {
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

	if err = waitKafkaIsUp(); err != nil {
		panic(err)
	}

	return resp.ID, nil
}
