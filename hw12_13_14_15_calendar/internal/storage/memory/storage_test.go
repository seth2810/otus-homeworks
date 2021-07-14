package memorystorage

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pioz/faker"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StorageTestSuite struct {
	suite.Suite
	storage *Storage
}

func (s *StorageTestSuite) BeforeTest(suiteName, testName string) {
	s.storage = New()
}

func (s *StorageTestSuite) TestEmpty() {
	events, err := s.storage.ListDayEvents(context.TODO(), time.Now())
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)

	events, err = s.storage.ListWeekEvents(context.TODO(), time.Now())
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)

	events, err = s.storage.ListMonthEvents(context.TODO(), time.Now())
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)
}

func (s *StorageTestSuite) TestCreate() {
	event := storage.Event{ID: faker.UUID()}

	require.NoError(s.T(), s.storage.CreateEvent(context.TODO(), event))
}

func (s *StorageTestSuite) TestCreateExists() {
	event := storage.Event{ID: faker.UUID()}

	s.storage.CreateEvent(context.TODO(), event)

	require.ErrorIs(s.T(), s.storage.CreateEvent(context.TODO(), event), errEventAlreadyExists)
}

func (s *StorageTestSuite) TestUpdateNotExist() {
	event := storage.Event{ID: faker.UUID()}

	require.ErrorIs(s.T(), s.storage.UpdateEvent(context.TODO(), event.ID, event), errEventNotFound)
}

func (s *StorageTestSuite) TestUpdate() {
	event := storage.Event{ID: faker.UUID()}

	s.storage.CreateEvent(context.TODO(), event)

	eventUpdate := storage.Event{ID: event.ID, Description: faker.String()}

	require.NoError(s.T(), s.storage.UpdateEvent(context.TODO(), event.ID, eventUpdate))
	require.NotEqual(s.T(), event, eventUpdate)
}

func (s *StorageTestSuite) TestDeleteNotExist() {
	event := storage.Event{ID: faker.UUID()}

	require.ErrorIs(s.T(), s.storage.DeleteEvent(context.TODO(), event.ID), errEventNotFound)
}

func (s *StorageTestSuite) TestDelete() {
	event := storage.Event{ID: faker.UUID()}

	s.storage.CreateEvent(context.TODO(), event)

	require.NoError(s.T(), s.storage.DeleteEvent(context.TODO(), event.ID))
	require.ErrorIs(s.T(), s.storage.DeleteEvent(context.TODO(), event.ID), errEventNotFound)
}

func (s *StorageTestSuite) TestList() {
	date := time.Date(2021, 6, 20, 0, 0, 0, 0, time.Local)
	event1 := storage.Event{ID: faker.UUID(), StartsAt: date.Add(90 * time.Minute)}
	event2 := storage.Event{ID: faker.UUID(), StartsAt: date.AddDate(0, 0, 2)}

	s.storage.CreateEvent(context.TODO(), event1)
	s.storage.CreateEvent(context.TODO(), event2)

	events, err := s.storage.ListMonthEvents(context.TODO(), date)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 2)
	require.Contains(s.T(), events, event1)
	require.Contains(s.T(), events, event2)

	events, err = s.storage.ListWeekEvents(context.TODO(), date)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 1)
	require.Contains(s.T(), events, event1)

	events, err = s.storage.ListDayEvents(context.TODO(), date)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 1)
	require.Contains(s.T(), events, event1)

	nextMonthDate := date.AddDate(0, 1, 0)

	events, err = s.storage.ListDayEvents(context.TODO(), nextMonthDate)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)

	events, err = s.storage.ListWeekEvents(context.TODO(), nextMonthDate)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)

	events, err = s.storage.ListMonthEvents(context.TODO(), nextMonthDate)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 0)
}

func (s *StorageTestSuite) TestConcurrency() {
	wg := &sync.WaitGroup{}
	wg.Add(3)

	eventsCount := 100
	deleteCh := make(chan string, eventsCount)
	updateCh := make(chan storage.Event, eventsCount)
	createCh := make(chan storage.Event, eventsCount)
	startsAt := time.Date(2021, 6, 20, 0, 0, 0, 0, time.Local)

	for i := 0; i < eventsCount; i++ {
		createCh <- storage.Event{ID: faker.UUID(), StartsAt: startsAt}
	}

	close(createCh)

	go func() {
		defer close(updateCh)
		defer wg.Done()

		for e := range createCh {
			require.NoError(s.T(), s.storage.CreateEvent(context.TODO(), e))

			<-time.After(time.Nanosecond * time.Duration(rand.Intn(1000)))
			e.Description = faker.String()
			updateCh <- e
		}
	}()

	go func() {
		defer close(deleteCh)
		defer wg.Done()

		for e := range updateCh {
			require.NoError(s.T(), s.storage.UpdateEvent(context.TODO(), e.ID, e))

			<-time.After(time.Nanosecond * time.Duration(rand.Intn(1000)))
			deleteCh <- e.ID
		}
	}()

	go func() {
		defer wg.Done()

		for id := range deleteCh {
			require.NoError(s.T(), s.storage.DeleteEvent(context.TODO(), id))
		}
	}()

	wg.Wait()

	events, _ := s.storage.ListDayEvents(context.TODO(), startsAt)

	require.Len(s.T(), events, 0)
}

func TestStorage(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}
