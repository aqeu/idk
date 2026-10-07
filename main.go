package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct {
	URLs           []string
	ProxyList      []string
	Concurrency    int
	RequestsPerURL int
	Timeout        time.Duration
	Retries        int
	RetryDelay     time.Duration
	RateLimit      int
	UserAgentFile  string
	OutputFile     string
	Verbose        bool
	FollowRedirect bool
	HTTP2          bool
}

type Metrics struct {
	totalRequests   atomic.Int64
	successRequests atomic.Int64
	failedRequests  atomic.Int64
	retriedRequests atomic.Int64
	totalBytes      atomic.Int64
	minLatency      atomic.Int64
	maxLatency      atomic.Int64
	totalLatency    atomic.Int64
	startTime       time.Time
	mu              sync.Mutex
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) Record(success bool, latency time.Duration, bytes int64) {
	m.totalRequests.Add(1)
	if success {
		m.successRequests.Add(1)
	} else {
		m.failedRequests.Add(1)
	}
	m.totalBytes.Add(bytes)
	m.totalLatency.Add(int64(latency))
	for {
		oldMin := m.minLatency.Load()
		if latency < time.Duration(oldMin) && m.minLatency.CompareAndSwap(oldMin, int64(latency)) {
			break
		}
		if latency >= time.Duration(oldMin) {
			break
		}
	}
	for {
		oldMax := m.maxLatency.Load()
		if latency > time.Duration(oldMax) && m.maxLatency.CompareAndSwap(oldMax, int64(latency)) {
			break
		}
		if latency <= time.Duration(oldMax) {
			break
		}
	}
}

func (m *Metrics) RecordRetry() {
	m.retriedRequests.Add(1)
}

func (m *Metrics) Summary() map[string]interface{} {
	total := m.totalRequests.Load()
	success := m.successRequests.Load()
	failed := m.failedRequests.Load()
	retried := m.retriedRequests.Load()
	bytes := m.totalBytes.Load()
	elapsed := time.Since(m.startTime)
	avgLatency := time.Duration(0)
	if total > 0 {
		avgLatency = time.Duration(m.totalLatency.Load() / total)
	}
	rps := float64(0)
	if elapsed.Seconds() > 0 {
		rps = float64(total) / elapsed.Seconds()
	}

	return map[string]interface{}{
		"total_requests":   total,
		"success_requests": success,
		"failed_requests":  failed,
		"retried_requests": retried,
		"total_bytes":      bytes,
		"min_latency_ms":   time.Duration(m.minLatency.Load()).Milliseconds(),
		"max_latency_ms":   time.Duration(m.maxLatency.Load()).Milliseconds(),
		"avg_latency_ms":   avgLatency.Milliseconds(),
		"requests_per_sec": rps,
		"elapsed_seconds":  elapsed.Seconds(),
		"success_rate":     fmt.Sprintf("%.2f%%", float64(success)/float64(total)*100),
	}
}

type UserAgentPool struct {
	agents []string
	idx    atomic.Uint64
}

func NewUserAgentPool(agents []string) *UserAgentPool {
	if len(agents) == 0 {
		agents = getDefaultUserAgents()
	}
	return &UserAgentPool{agents: agents}
}

func (p *UserAgentPool) Next() string {
	if len(p.agents) == 0 {
		return getDefaultUserAgents()[0]
	}
	idx := p.idx.Add(1)
	return p.agents[idx%uint64(len(p.agents))]
}

func (p *UserAgentPool) Random() string {
	if len(p.agents) == 0 {
		return getDefaultUserAgents()[0]
	}
	return p.agents[rand.Intn(len(p.agents))]
}

func getDefaultUserAgents() []string {
	return []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:124.0) Gecko/20100101 Firefox/124.0",
		"Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
		"Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
		"Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/124.0.0.0 Mobile/15E148 Safari/604.1",
	}
}

type ProxyPool struct {
	proxies []string
	idx     atomic.Uint64
}

func NewProxyPool(proxies []string) *ProxyPool {
	return &ProxyPool{proxies: proxies}
}

func (p *ProxyPool) Next() (string, error) {
	if len(p.proxies) == 0 {
		return "", errors.New("no proxies")
	}
	idx := p.idx.Add(1)
	return p.proxies[idx%uint64(len(p.proxies))], nil
}

func (p *ProxyPool) Random() (string, error) {
	if len(p.proxies) == 0 {
		return "", errors.New("no proxies")
	}
	return p.proxies[rand.Intn(len(p.proxies))], nil
}

func (p *ProxyPool) Size() int {
	return len(p.proxies)
}

type ClientCache struct {
	mu      sync.Mutex
	clients map[string]*http.Client
	config  *Config
	jar     *cookiejar.Jar
}

func NewClientCache(cfg *Config) *ClientCache {
	jar, _ := cookiejar.New(&cookiejar.Options{})
	return &ClientCache{
		clients: make(map[string]*http.Client),
		config:  cfg,
		jar:     jar,
	}
}

func (c *ClientCache) GetClient(proxyURL string) (*http.Client, error) {
	key := proxyURL
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[key]; ok {
		return client, nil
	}

	var transport http.RoundTripper
	tlsConfig := &tls.Config{
		MinVersion:       tls.VersionTLS12,
		MaxVersion:       tls.VersionTLS13,
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384},
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
		InsecureSkipVerify: false,
	}

	if proxyURL != "" {
		proxy, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport = &http.Transport{
			Proxy:           http.ProxyURL(proxy),
			TLSClientConfig: tlsConfig,
			DialContext: (&net.Dialer{
				Timeout:   c.config.Timeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: c.config.Timeout,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
			ForceAttemptHTTP2:     c.config.HTTP2,
		}
	} else {
		transport = &http.Transport{
			TLSClientConfig: tlsConfig,
			DialContext: (&net.Dialer{
				Timeout:   c.config.Timeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: c.config.Timeout,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
			ForceAttemptHTTP2:     c.config.HTTP2,
		}
	}

	client := &http.Client{
		Timeout:   c.config.Timeout,
		Transport: transport,
		Jar:       c.jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !c.config.FollowRedirect {
				return http.ErrUseLastResponse
			}
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}

	c.clients[key] = client
	return client, nil
}

type RequestBuilder struct {
	userAgentPool *UserAgentPool
}

func NewRequestBuilder(uaPool *UserAgentPool) *RequestBuilder {
	return &RequestBuilder{userAgentPool: uaPool}
}

func (b *RequestBuilder) BuildRequest(ctx context.Context, method, targetURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}

	ua := b.userAgentPool.Next()
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", getRandomAccept())
	req.Header.Set("Accept-Language", getRandomAcceptLanguage())
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", getRandomSecFetchDest())
	req.Header.Set("Sec-Fetch-Mode", getRandomSecFetchMode())
	req.Header.Set("Sec-Fetch-Site", getRandomSecFetchSite())
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	if rand.Intn(2) == 0 {
		req.Header.Set("DNT", "1")
	}
	if rand.Intn(3) == 0 {
		req.Header.Set("Referer", getRandomReferer(targetURL))
	}
	return req, nil
}

func getRandomAccept() string {
	accepts := []string{
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
		"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
		"application/json, text/plain, */*",
	}
	return accepts[rand.Intn(len(accepts))]
}

func getRandomAcceptLanguage() string {
	langs := []string{
		"en-US,en;q=0.9",
		"en-GB,en;q=0.9",
		"en-US,en;q=0.9,es;q=0.8",
		"en-US,en;q=0.9,fr;q=0.8",
		"en-US,en;q=0.9,de;q=0.8",
		"en-US,en;q=0.9,ja;q=0.8",
		"en-US,en;q=0.9,zh-CN;q=0.8",
		"en-US,en;q=0.9,ko;q=0.8",
		"en-US,en;q=0.9,pt-BR;q=0.8",
		"en-US,en;q=0.9,ru;q=0.8",
	}
	return langs[rand.Intn(len(langs))]
}

func getRandomSecFetchDest() string {
	dests := []string{"document", "empty", "iframe", "script", "style", "image", "font", "object", "audio", "video"}
	return dests[rand.Intn(len(dests))]
}

func getRandomSecFetchMode() string {
	modes := []string{"navigate", "no-cors", "cors", "same-origin"}
	return modes[rand.Intn(len(modes))]
}

func getRandomSecFetchSite() string {
	sites := []string{"none", "same-origin", "same-site", "cross-site"}
	return sites[rand.Intn(len(sites))]
}

func getRandomReferer(targetURL string) string {
	referers := []string{
		"https://www.google.com/",
		"https://www.google.com/search?q=" + url.QueryEscape(targetURL),
		"https://www.bing.com/search?q=" + url.QueryEscape(targetURL),
		"https://duckduckgo.com/?q=" + url.QueryEscape(targetURL),
		"https://www.reddit.com/",
		"https://news.ycombinator.com/",
		"https://twitter.com/",
		"https://www.linkedin.com/",
		"https://github.com/",
	}
	return referers[rand.Intn(len(referers))]
}

type Job struct {
	URL    string
	Method string
	Proxy  string
}

type Result struct {
	URL        string
	Success    bool
	Latency    time.Duration
	Bytes      int64
	StatusCode int
	Error      error
	Attempt    int
}

type WorkerPool struct {
	workers     int
	jobQueue    chan Job
	resultQueue chan Result
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	clientCache *ClientCache
	builder     *RequestBuilder
	metrics     *Metrics
	config      *Config
	rateLimiter *RateLimiter
}

func NewWorkerPool(workers int, clientCache *ClientCache, builder *RequestBuilder, metrics *Metrics, cfg *Config) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		workers:     workers,
		jobQueue:    make(chan Job, workers*4),
		resultQueue: make(chan Result, workers*4),
		ctx:         ctx,
		cancel:      cancel,
		clientCache: clientCache,
		builder:     builder,
		metrics:     metrics,
		config:      cfg,
		rateLimiter: NewRateLimiter(cfg.RateLimit),
	}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.jobQueue:
			if !ok {
				return
			}
			p.rateLimiter.Wait()
			result := p.processJobWithRetries(job)
			select {
			case p.resultQueue <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *WorkerPool) processJobWithRetries(job Job) Result {
	var lastResult Result
	for attempt := 0; attempt <= p.config.Retries; attempt++ {
		start := time.Now()
		client, err := p.clientCache.GetClient(job.Proxy)
		if err != nil {
			lastResult = Result{URL: job.URL, Success: false, Error: err, Attempt: attempt}
			break
		}

		req, err := p.builder.BuildRequest(p.ctx, job.Method, job.URL)
		if err != nil {
			lastResult = Result{URL: job.URL, Success: false, Error: err, Attempt: attempt}
			break
		}
		resp, err := client.Do(req)
		latency := time.Since(start)
		if err != nil {
			p.metrics.Record(false, latency, 0)
			lastResult = Result{URL: job.URL, Success: false, Latency: latency, Error: err, Attempt: attempt}
			if attempt < p.config.Retries {
				p.metrics.RecordRetry()
				time.Sleep(exponentialBackoff(attempt, p.config.RetryDelay))
				if len(p.config.ProxyList) > 0 {
					job.Proxy, _ = p.randomProxy()
				}
				continue
			}
			break
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		bytesRead := int64(len(body))
		success := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 400
		p.metrics.Record(success, latency, bytesRead)
		lastResult = Result{
			URL:        job.URL,
			Success:    success,
			Latency:    latency,
			Bytes:      bytesRead,
			StatusCode: resp.StatusCode,
			Error:      err,
			Attempt:    attempt,
		}
		if success || attempt == p.config.Retries {
			break
		}
		p.metrics.RecordRetry()
		time.Sleep(exponentialBackoff(attempt, p.config.RetryDelay))
		if len(p.config.ProxyList) > 0 {
			job.Proxy, _ = p.randomProxy()
		}
	}
	return lastResult
}

func (p *WorkerPool) randomProxy() (string, error) {
	return "", nil
}

func (p *WorkerPool) Submit(job Job) {
	select {
	case p.jobQueue <- job:
	case <-p.ctx.Done():
	}
}

func (p *WorkerPool) Results() <-chan Result {
	return p.resultQueue
}

func (p *WorkerPool) Stop() {
	p.cancel()
	close(p.jobQueue)
	p.wg.Wait()
	close(p.resultQueue)
}

type RateLimiter struct {
	enabled  bool
	interval time.Duration
	tokens   chan struct{}
	stop     chan struct{}
	once     sync.Once
}

func NewRateLimiter(rate int) *RateLimiter {
	if rate <= 0 {
		return &RateLimiter{enabled: false}
	}
	interval := time.Second / time.Duration(rate)
	rl := &RateLimiter{
		enabled:  true,
		interval: interval,
		tokens:   make(chan struct{}, rate),
		stop:     make(chan struct{}),
	}
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}
	go rl.refill()
	return rl
}

func (r *RateLimiter) refill() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case r.tokens <- struct{}{}:
			default:
			}
		case <-r.stop:
			return
		}
	}
}

func (r *RateLimiter) Wait() {
	if !r.enabled {
		return
	}
	<-r.tokens
}

func (r *RateLimiter) Stop() {
	r.once.Do(func() {
		close(r.stop)
	})
}

func exponentialBackoff(attempt int, baseDelay time.Duration) time.Duration {
	delay := baseDelay * time.Duration(math.Pow(2, float64(attempt)))
	jitter := time.Duration(rand.Int63n(int64(baseDelay)))
	return delay + jitter
}

func loadUserAgents(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var agents []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			agents = append(agents, line)
		}
	}
	return agents, scanner.Err()
}

func loadProxies(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			if !strings.HasPrefix(line, "http://") && !strings.HasPrefix(line, "https://") && !strings.HasPrefix(line, "socks5://") {
				line = "http://" + line
			}
			proxies = append(proxies, line)
		}
	}
	return proxies, scanner.Err()
}

func loadURLs(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			urls = append(urls, line)
		}
	}
	return urls, scanner.Err()
}

func printBanner() {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║        ██████╗  ██████╗ ██████╗ ███████╗██████╗           ║
║        ██╔══██╗██╔═══██╗██╔══██╗██╔════╝██╔══██╗          ║
║        ██████╔╝██║   ██║██████╔╝█████╗  ██║  ██║          ║
║        ██╔══██╗██║   ██║██╔══██╗██╔══╝  ██║  ██║          ║
║        ██████╔╝╚██████╔╝██║  ██║███████╗██████╔╝          ║
║        ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═════╝           ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}

func printConfig(cfg *Config) {
	fmt.Println("\n[CONFIGURATION]")
	fmt.Printf("  URLs:            %d\n", len(cfg.URLs))
	fmt.Printf("  Proxies:         %d\n", len(cfg.ProxyList))
	fmt.Printf("  Concurrency:     %d workers\n", cfg.Concurrency)
	fmt.Printf("  Requests/URL:    %d\n", cfg.RequestsPerURL)
	fmt.Printf("  Timeout:         %v\n", cfg.Timeout)
	fmt.Printf("  Retries:         %d\n", cfg.Retries)
	fmt.Printf("  Retry Delay:     %v\n", cfg.RetryDelay)
	fmt.Printf("  Rate Limit:      %d req/s (0=unlimited)\n", cfg.RateLimit)
	fmt.Printf("  HTTP/2:          %v\n", cfg.HTTP2)
	fmt.Printf("  Follow Redirect: %v\n", cfg.FollowRedirect)
	fmt.Printf("  Verbose:         %v\n", cfg.Verbose)
	fmt.Println()
}

func printMetrics(metrics *Metrics) {
	summary := metrics.Summary()
	data, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println("\n[METRICS]")
	fmt.Println(string(data))
}

func run(cfg *Config) error {
	printBanner()
	printConfig(cfg)
	rand.Seed(time.Now().UnixNano())
	metrics := NewMetrics()
	var uaPool *UserAgentPool
	if cfg.UserAgentFile != "" {
		agents, err := loadUserAgents(cfg.UserAgentFile)
		if err != nil {
			fmt.Printf("failed to load user agents: %v\n", err)
			uaPool = NewUserAgentPool(nil)
		} else {
			uaPool = NewUserAgentPool(agents)
		}
	} else {
		uaPool = NewUserAgentPool(nil)
	}
	proxyPool := NewProxyPool(cfg.ProxyList)
	clientCache := NewClientCache(cfg)
	builder := NewRequestBuilder(uaPool)
	pool := NewWorkerPool(cfg.Concurrency, clientCache, builder, metrics, cfg)
	pool.rateLimiter = NewRateLimiter(cfg.RateLimit)
	pool.Start()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	totalRequests := int64(len(cfg.URLs) * cfg.RequestsPerURL)
	fmt.Printf("[STARTING] processing %d total requests...\n\n", totalRequests)
	done := make(chan struct{})
	var completed int64
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c := atomic.LoadInt64(&completed)
				if c > 0 && c%100 == 0 {
					fmt.Printf("[PROGRESS] %d/%d requests completed (%.1f%%)\n",
						c, totalRequests, float64(c)/float64(totalRequests)*100)
				}
			case <-done:
				return
			}
		}
	}()
	go func() {
		for _, targetURL := range cfg.URLs {
			for i := 0; i < cfg.RequestsPerURL; i++ {
				proxy := ""
				if proxyPool.Size() > 0 {
					p, err := proxyPool.Next()
					if err == nil {
						proxy = p
					}
				}
				job := Job{
					URL:    targetURL,
					Method: "GET",
					Proxy:  proxy,
				}
				pool.Submit(job)
			}
		}
	}()
	resultsCollected := int64(0)
	for resultsCollected < totalRequests {
		select {
		case result := <-pool.Results():
			resultsCollected++
			completed++
			if cfg.Verbose {
				status := "OK"
				if !result.Success {
					status = "FAIL"
				}
				fmt.Printf("[%s] %s | %dms | %d bytes | attempt %d | %v\n",
					status, result.URL, result.Latency.Milliseconds(),
					result.Bytes, result.Attempt, result.Error)
			}
		case <-sigChan:
			fmt.Println("\n\n[SHUTDOWN] Received interrupt signal, shutting down gracefully...")
			close(done)
			pool.Stop()
			pool.rateLimiter.Stop()
			printMetrics(metrics)
			return nil
		}
	}
	close(done)
	pool.Stop()
	pool.rateLimiter.Stop()
	if cfg.OutputFile != "" {
		summary := metrics.Summary()
		data, err := json.MarshalIndent(summary, "", "  ")
		if err == nil {
			if err := os.WriteFile(cfg.OutputFile, data, 0644); err != nil {
				fmt.Printf("could not write output file: %v\n", err)
			}
		}
	}

	printMetrics(metrics)
	return nil
}

func main() {
	cfg := &Config{}
	flag.StringVar(&cfg.UserAgentFile, "ua-file", "", "Path to user-agent list file (one per line)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file for results (JSON)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 50, "Number of concurrent workers")
	flag.IntVar(&cfg.RequestsPerURL, "requests", 1, "Number of requests per URL")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "Request timeout")
	flag.IntVar(&cfg.Retries, "retries", 3, "Number of retries on failure")
	flag.DurationVar(&cfg.RetryDelay, "retry-delay", 1*time.Second, "Base delay for retries")
	flag.IntVar(&cfg.RateLimit, "rate-limit", 0, "Rate limit (requests per second, 0 = unlimited)")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&cfg.FollowRedirect, "follow-redirect", true, "Follow redirects")
	flag.BoolVar(&cfg.HTTP2, "http2", true, "Enable HTTP/2")
	flag.Parse()
	args := flag.Args()
	if len(args) > 0 {
		cfg.URLs = args
	} else {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line != "" {
					cfg.URLs = append(cfg.URLs, line)
				}
			}
		}
	}
	if len(cfg.URLs) == 0 {
		fmt.Fprintln(os.Stderr, "no URLs provided. use as: <url1> <url2> ... or pipe URLs via stdin")
		os.Exit(1)
	}
	if proxyFile := os.Getenv("PROXY_FILE"); proxyFile != "" {
		proxies, err := loadProxies(proxyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load proxies from %s: %v\n", proxyFile, err)
		} else {
			cfg.ProxyList = append(cfg.ProxyList, proxies...)
		}
	}
	if proxyEnv := os.Getenv("HTTP_PROXY"); proxyEnv != "" {
		cfg.ProxyList = append(cfg.ProxyList, proxyEnv)
	}
	if proxyEnv := os.Getenv("HTTPS_PROXY"); proxyEnv != "" {
		cfg.ProxyList = append(cfg.ProxyList, proxyEnv)
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Concurrency > 1000 {
		cfg.Concurrency = 1000
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.RateLimit < 0 {
		cfg.RateLimit = 0
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
