package redisstringset

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

type nothing struct{}

type Set struct {
	sync.Mutex
	redisClient *redis.Client
	key         string
}

// New returns a Set backed by Redis, containing the values provided in the arguments.
func New(redisClient *redis.Client, key string, initial ...string) *Set {
	s := &Set{
		redisClient: redisClient,
		key:         key,
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

	s.redisClient.Del(context.Background(), s.key)
}

// Has returns true if the receiver Set already contains the element string argument.
func (s *Set) Has(element string) bool {
	s.Lock()
	defer s.Unlock()

	result, _ := s.redisClient.SIsMember(context.Background(), s.key, strings.ToLower(element)).Result()
	return result
}

// Insert adds the element string argument to the receiver Set.
func (s *Set) Insert(element string) {
	s.Lock()
	defer s.Unlock()

	s.redisClient.SAdd(context.Background(), s.key, strings.ToLower(element))
}

// InsertMany adds all the elements strings into the receiver Set.
func (s *Set) InsertMany(elements ...string) {
	for _, i := range elements {
		s.Insert(i)
	}
}

// Remove will delete the element string from the receiver Set.
func (s *Set) Remove(element string) {
	s.Lock()
	defer s.Unlock()

	s.redisClient.SRem(context.Background(), s.key, strings.ToLower(element))
}

// Slice returns a string slice that contains all the elements in the Set.
func (s *Set) Slice() []string {
	s.Lock()
	defer s.Unlock()

	result, _ := s.redisClient.SMembers(context.Background(), s.key).Result()
	return result
}

// Union adds all the elements from the other Set argument into the receiver Set.
func (s *Set) Union(other *Set) {
	for _, item := range other.Slice() {
		s.Insert(item)
	}
}

// Len returns the number of elements in the receiver Set.
func (s *Set) Len() int {
	s.Lock()
	defer s.Unlock()

	result, _ := s.redisClient.SCard(context.Background(), s.key).Result()
	return int(result)
}

// Subtract removes all elements in the other Set argument from the receiver Set.
func (s *Set) Subtract(other *Set) {
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
	return strings.Join(s.Slice(), ",")
}

// Set implements the flag.Value interface.
func (s *Set) Set(input string) error {
	if input == "" {
		return fmt.Errorf("String parsing failed")
	}

	for _, item := range strings.Split(input, ",") {
		s.Insert(strings.TrimSpace(item))
	}
	return nil
}
