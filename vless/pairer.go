package vless

import (
	"io"
	"net/netip"
	"os/exec"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type session struct {
	ip           netip.Addr
	immuneBefore time.Time
	expiresAt    time.Time
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

func (c *ProfileCache) IsImmune(uuid string, ip netip.Addr) (bool, bool) {
	session, ex := c.lru.Get(uuid)

	if !ex {
		return true, true
	}

	diffIp, isImmune := session.ip != ip, session.immuneBefore.After(time.Now().UTC())

	if diffIp && !isImmune {
		c.lru.Remove(uuid)
		go func(ip netip.Addr) {
			cmd := exec.Command("ss", "-K", "dst", ip.String())
			cmd.Stdout = io.Discard
			cmd.Stderr = io.Discard
			cmd.Run()
		}(ip)
	}

	return diffIp, isImmune
}

func (c *ProfileCache) Pair(uuid string, ip netip.Addr) {
	if !c.IsCached(uuid) {
		c.lru.Add(uuid, session{
			ip:           ip,
			immuneBefore: time.Now().UTC().Add(time.Second * time.Duration(10)),
			expiresAt:    time.Now().UTC().Add(time.Minute * time.Duration(10)),
		})
	}
}
