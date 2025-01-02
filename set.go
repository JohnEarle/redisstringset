package redisstringset

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

type nothing struct{}

type Set struct {
	sync.Mutex
	redisClient *redis.Client
	key         string
	logger      *log.Logger
}

// New returns a Set backed by Redis, containing the values provided in the arguments.
func New(redisClient *redis.Client, key string, initial ...string) *Set {
	logger := log.New(os.Stdout, "RedisSet: ", log.LstdFlags)
	s := &Set{
		redisClient: redisClient,
		key:         key,
		logger:      logger,
	}

	if len(initial) > 0 {
		s.InsertMany(initial...)
	}
	return s
}

// Deduplicate utilizes the Set type to generate a unique list of strings from the input slice.
func Deduplicate(redisClient *redis.Client, key string, input []string) []string {
	ss := New(redisClient, key, input...)
	defer ss.Close()

	return ss.Slice()
}

func (s *Set) Close() {
	s.Lock()
	defer s.Unlock()

	if _, err := s.redisClient.Del(context.Background(), s.key).Result(); err != nil {
		s.logger.Printf("Error deleting key %s: %v", s.key, err)
	}
}

// Has returns true if the receiver Set already contains the element string argument.
func (s *Set) Has(element string) bool {
	s.Lock()
	defer s.Unlock()

	result, err := s.redisClient.SIsMember(context.Background(), s.key, strings.ToLower(element)).Result()
	if err != nil {
		s.logger.Printf("Error checking membership for %s: %v", element, err)
		return false
	}
	return result
}

// Insert adds the element string argument to the receiver Set.
func (s *Set) Insert(element string) {
	s.Lock()
	defer s.Unlock()

	if _, err := s.redisClient.SAdd(context.Background(), s.key, strings.ToLower(element)).Result(); err != nil {
		s.logger.Printf("Error inserting %s into %s: %v", element, s.key, err)
	}
}

// InsertMany adds all the elements strings into the receiver Set.
func (s *Set) InsertMany(elements ...string) {
	s.Lock()
	defer s.Unlock()
	for _, i := range elements {

		s.Insert(i)
	}
}

// Remove will delete the element string from the receiver Set.
func (s *Set) Remove(element string) {
	s.Lock()
	defer s.Unlock()

	if _, err := s.redisClient.SRem(context.Background(), s.key, strings.ToLower(element)).Result(); err != nil {
		s.logger.Printf("Error removing %s from %s: %v", element, s.key, err)
	}
}

// Slice returns a string slice that contains all the elements in the Set.
func (s *Set) Slice() []string {
	s.Lock()
	defer s.Unlock()

	result, err := s.redisClient.SMembers(context.Background(), s.key).Result()
	if err != nil {
		s.logger.Printf("Error retrieving members for %s: %v", s.key, err)
		return []string{}
	}
	return result
}

// Union adds all the elements from the other Set argument into the receiver Set.
func (s *Set) Union(other *Set) {
	s.Lock()
	defer s.Unlock()
	for _, item := range other.Slice() {
		s.Insert(item)
	}
}

// Len returns the number of elements in the receiver Set.
func (s *Set) Len() int {
	s.Lock()
	defer s.Unlock()

	result, err := s.redisClient.SCard(context.Background(), s.key).Result()
	if err != nil {
		s.logger.Printf("Error getting length of %s: %v", s.key, err)
		return 0
	}
	return int(result)
}

// Subtract removes all elements in the other Set argument from the receiver Set.
func (s *Set) Subtract(other *Set) {
	s.Lock()
	defer s.Unlock()
	for _, item := range other.Slice() {
		s.Remove(item)
	}
}

// Intersect causes the receiver Set to only contain elements also found in the
// other Set argument.
func (s *Set) Intersect(other *Set) {
	s.Lock()
	defer s.Unlock()

	members := s.Slice()
	for _, item := range members {
		if !other.Has(item) {
			s.Remove(item)
		}
	}
}

// String implements the flag.Value interface.
func (s *Set) String() string {
	s.Lock()
	defer s.Unlock()
	return strings.Join(s.Slice(), ",")
}

// Set implements the flag.Value interface.
func (s *Set) Set(input string) error {
	s.Lock()
	defer s.Unlock()
	if input == "" {
		return fmt.Errorf("string parsing failed")
	}

	for _, item := range strings.Split(input, ",") {
		s.Insert(strings.TrimSpace(item))
	}
	return nil
}
