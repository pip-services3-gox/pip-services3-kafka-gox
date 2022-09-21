package test_connect

import (
	"context"
	"os"
	"testing"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	connect "github.com/pip-services3-gox/pip-services3-kafka-gox/connect"
	"github.com/stretchr/testify/assert"
)

type kafkaConnectionTest struct {
	connection *connect.KafkaConnection
}

func newKafkaConnectionTest() *kafkaConnectionTest {
	kafkaUri := os.Getenv("KAFKA_SERVICE_URI")
	kafkaHost := os.Getenv("KAFKA_SERVICE_HOST")
	if kafkaHost == "" {
		kafkaHost = "localhost"
	}

	kafkaPort := os.Getenv("KAFKA_SERVICE_PORT")
	if kafkaPort == "" {
		kafkaPort = "9092"
	}

	kafkaUser := os.Getenv("KAFKA_USER")
	// if kafkaUser == "" {
	// 	kafkaUser = ""
	// }
	kafkaPassword := os.Getenv("KAFKA_PASS")
	// if kafkaPassword == "" {
	// 	kafkaPassword = ""
	// }

	if kafkaUri == "" && kafkaHost == "" {
		return nil
	}

	connection := connect.NewKafkaConnection()
	connection.Configure(context.Background(),
		cconf.NewConfigParamsFromTuples(
			"connection.uri", kafkaUri,
			"connection.host", kafkaHost,
			"connection.port", kafkaPort,
			"credential.mechanism", "plain",
			"credential.username", kafkaUser,
			"credential.password", kafkaPassword,
		),
	)

	return &kafkaConnectionTest{
		connection: connection,
	}
}

func (c *kafkaConnectionTest) TestOpenClose(t *testing.T) {
	err := c.connection.Open(context.Background(), "")
	assert.Nil(t, err)
	assert.True(t, c.connection.IsOpen())
	assert.NotNil(t, c.connection.GetConnection())

	err = c.connection.Close(context.Background(), "")
	assert.Nil(t, err)
	assert.False(t, c.connection.IsOpen())
	assert.Nil(t, c.connection.GetConnection())
}

func (c *kafkaConnectionTest) TestReadTopics(t *testing.T) {
	err := c.connection.Open(context.Background(), "")
	assert.Nil(t, err)
	assert.True(t, c.connection.IsOpen())
	assert.NotNil(t, c.connection.GetConnection())

	topics, err := c.connection.ReadQueueNames()
	assert.Nil(t, err)
	assert.NotNil(t, topics)

	err = c.connection.Close(context.Background(), "")
	assert.Nil(t, err)
	assert.False(t, c.connection.IsOpen())
	assert.Nil(t, c.connection.GetConnection())
}

func TestKafkaConnection(t *testing.T) {
	c := newKafkaConnectionTest()
	if c == nil {
		return
	}

	t.Run("Open and Close", c.TestOpenClose)
	t.Run("Read Topics", c.TestReadTopics)
}
