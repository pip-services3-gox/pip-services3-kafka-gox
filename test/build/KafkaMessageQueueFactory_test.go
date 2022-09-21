package test_build

import (
	"testing"

	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	build "github.com/pip-services3-gox/pip-services3-kafka-gox/build"
	queues "github.com/pip-services3-gox/pip-services3-kafka-gox/queues"
	"github.com/stretchr/testify/assert"
)

func TestKafkaMessageQueueFactory(t *testing.T) {
	factory := build.NewKafkaMessageQueueFactory()
	descriptor := cref.NewDescriptor("pip-services", "message-queue", "kafka", "test", "1.0")

	canResult := factory.CanCreate(descriptor)
	assert.NotNil(t, canResult)

	comp, err := factory.Create(descriptor)
	assert.Nil(t, err)
	assert.NotNil(t, comp)

	queue := comp.(*queues.KafkaMessageQueue)
	assert.Equal(t, "test", queue.Name())
}
