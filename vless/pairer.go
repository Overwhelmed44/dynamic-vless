package vless

import (
	"net/netip"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type session struct {
	ip        netip.Addr
	expiresAt time.Time
}

type ProfileCache struct {
	lru *lru.Cache[string, session]
}

func NewProfileCache(size int) *ProfileCache {
	c, _ := lru.New[string, session](size)

	return &ProfileCache{lru: c}
}

func (c *ProfileCache) IsCached(uuid string) bool {
	session, ex := c.lru.Get(uuid)

	if !ex {
		return false
	}
	if session.expiresAt.After(time.Now().UTC()) {
		return true
	}

	c.lru.Remove(uuid)

	return false
}

func (c *ProfileCache) Pair(uuid string, ip netip.Addr, ok bool) {
	var dur time.Duration

	if ok {
		dur = time.Minute * time.Duration(10)
	} else {
		dur = time.Second * time.Duration(5)
	}

	if !c.IsCached(uuid) {
		c.lru.Add(uuid, session{
			ip:        ip,
			expiresAt: time.Now().UTC().Add(dur),
		})
	}
}
