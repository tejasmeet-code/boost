import aiohttp
import asyncio
import sys
import random
import json
import time
import argparse
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class BoostResult:
    """Track individual token boost results"""
    token: str
    status: str
    timestamp: float
    error_msg: Optional[str] = None
    retry_count: int = 0


@dataclass
class RateLimitInfo:
    """Track rate limit buckets"""
    endpoint: str
    reset_at: float
    retry_after: float
    global_rl: bool = False


class DiscordBooster:
    """Advanced Discord Guild Booster with retry logic, proxy support, and analytics"""

    def __init__(
        self,
        guild_id: str,
        tokens: List[str],
        max_concurrent: int = 3,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        use_proxies: bool = False,
        proxy_list: Optional[List[str]] = None,
        randomize_delays: bool = True,
        delay_range: tuple = (1.5, 4.0),
        verbose: bool = False
    ):
        self.guild_id = guild_id
        self.tokens = list(dict.fromkeys(tokens))  # Remove duplicates while preserving order
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.use_proxies = use_proxies
        self.proxy_list = proxy_list or []
        self.randomize_delays = randomize_delays
        self.delay_range = delay_range
        self.verbose = verbose

        # State tracking
        self.results: List[BoostResult] = []
        self.rate_limits: Dict[str, RateLimitInfo] = {}
        self.active_tokens: Set[str] = set()
        self.failed_tokens: Set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem: Optional[asyncio.Semaphore] = None
        self.stats = {
            'boosted': 0,
            'already_boosting': 0,
            'failed': 0,
            'rate_limited': 0,
            'errors': 0,
            'retried': 0
        }

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Generate request headers with realistic fingerprinting"""
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Discord-Locale": "en-US",
            "X-Discord-Timezone": "America/New_York",
            "Origin": "https://discord.com",
            "Referer": f"https://discord.com/channels/{self.guild_id}",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin"
        }

    def _get_proxy(self) -> Optional[str]:
        """Get random proxy from list if enabled"""
        if not self.use_proxies or not self.proxy_list:
            return None
        return random.choice(self.proxy_list)

    def _mask_token(self, token: str) -> str:
        """Mask token for safe logging"""
        if len(token) <= 10:
            return "***"
        return f"{token[:6]}...{token[-4:]}"

    async def _handle_rate_limit(self, response: aiohttp.ClientResponse, token: str) -> float:
        """Extract and handle rate limit information"""
        retry_after = 5.0
        try:
            data = await response.json()
            retry_after = data.get('retry_after', 5.0)
        except:
            retry_after = float(response.headers.get('Retry-After', 5.0))

        is_global = response.headers.get('X-RateLimit-Global', 'false').lower() == 'true'
        bucket = response.headers.get('X-RateLimit-Bucket', 'unknown')

        self.rate_limits[bucket] = RateLimitInfo(
            endpoint=bucket,
            reset_at=time.time() + retry_after,
            retry_after=retry_after,
            global_rl=is_global
        )

        logger.warning(f"[RATELIMIT] {self._mask_token(token)} | Bucket: {bucket} | Retry: {retry_after}s | Global: {is_global}")
        return retry_after

    async def _boost_single(
        self, 
        token: str, 
        attempt: int = 0
    ) -> str:
        """Execute boost request with retry logic"""
        proxy = self._get_proxy()
        headers = self._get_headers(token)

        try:
            async with self.sem:
                # Apply delay before request
                if self.randomize_delays:
                    delay = random.uniform(*self.delay_range)
                    if attempt > 0:
                        delay += self.retry_delay * attempt
                    await asyncio.sleep(delay)

                payload = {
                    "user_premium_guild_subscription_slot_ids": []
                }

                timeout = aiohttp.ClientTimeout(total=15, connect=5)

                async with self.session.post(
                    f"https://discord.com/api/v9/guilds/{self.guild_id}/premium/subscriptions",
                    headers=headers,
                    json=payload,
                    timeout=timeout,
                    proxy=proxy
                ) as resp:

                    if resp.status == 201:
                        data = await resp.json()
                        subscription = data.get('premium_guild_subscription', {})
                        ended_at = subscription.get('ended_at')

                        status_msg = "Active" if ended_at is None else f"Until {ended_at}"
                        logger.info(f"[BOOSTED] {self._mask_token(token)} | Status: {status_msg}")
                        self.stats['boosted'] += 1
                        self.active_tokens.add(token)
                        return "boosted"

                    elif resp.status == 400:
                        try:
                            err = await resp.json()
                            err_str = str(err).lower()

                            if any(phrase in err_str for phrase in ["already used", "already boosting", "already subscribed"]):
                                logger.info(f"[ALREADY] {self._mask_token(token)} | Already boosting this server")
                                self.stats['already_boosting'] += 1
                                self.active_tokens.add(token)
                                return "already"

                            elif "no available" in err_str or "no slots" in err_str:
                                logger.warning(f"[NOSLOTS] {self._mask_token(token)} | No boost slots available")
                                self.stats['failed'] += 1
                                return "no_slots"

                            elif "invalid" in err_str or "unauthorized" in err_str:
                                logger.error(f"[INVALID] {self._mask_token(token)} | Token invalid or expired")
                                self.stats['failed'] += 1
                                self.failed_tokens.add(token)
                                return "invalid"

                            else:
                                logger.error(f"[FAIL] {self._mask_token(token)} | {err}")
                                self.stats['failed'] += 1
                                return "fail"

                        except Exception as e:
                            logger.error(f"[FAIL] {self._mask_token(token)} | Parse error: {e}")
                            self.stats['failed'] += 1
                            return "fail"

                    elif resp.status == 401:
                        logger.error(f"[UNAUTHORIZED] {self._mask_token(token)} | Token invalid")
                        self.stats['failed'] += 1
                        self.failed_tokens.add(token)
                        return "invalid"

                    elif resp.status == 403:
                        logger.error(f"[FORBIDDEN] {self._mask_token(token)} | Missing permissions")
                        self.stats['failed'] += 1
                        return "forbidden"

                    elif resp.status == 429:
                        retry_after = await self._handle_rate_limit(resp, token)
                        self.stats['rate_limited'] += 1

                        if attempt < self.max_retries:
                            self.stats['retried'] += 1
                            logger.info(f"[RETRY] {self._mask_token(token)} | Attempt {attempt + 1}/{self.max_retries}")
                            await asyncio.sleep(retry_after)
                            return await self._boost_single(token, attempt + 1)

                        return "ratelimit"

                    elif resp.status >= 500:
                        logger.warning(f"[SERVER_ERROR] {self._mask_token(token)} | Status {resp.status}")
                        if attempt < self.max_retries:
                            self.stats['retried'] += 1
                            await asyncio.sleep(self.retry_delay * (attempt + 1))
                            return await self._boost_single(token, attempt + 1)
                        self.stats['failed'] += 1
                        return "server_error"

                    else:
                        body = await resp.text()
                        logger.error(f"[FAIL {resp.status}] {self._mask_token(token)} | {body[:200]}")
                        self.stats['failed'] += 1
                        return "fail"

        except asyncio.TimeoutError:
            logger.error(f"[TIMEOUT] {self._mask_token(token)} | Request timed out")
            if attempt < self.max_retries:
                self.stats['retried'] += 1
                await asyncio.sleep(self.retry_delay)
                return await self._boost_single(token, attempt + 1)
            self.stats['errors'] += 1
            return "timeout"

        except aiohttp.ClientError as e:
            logger.error(f"[NETWORK] {self._mask_token(token)} | {e}")
            if attempt < self.max_retries:
                self.stats['retried'] += 1
                await asyncio.sleep(self.retry_delay)
                return await self._boost_single(token, attempt + 1)
            self.stats['errors'] += 1
            return "network"

        except Exception as e:
            logger.error(f"[ERROR] {self._mask_token(token)} | {type(e).__name__}: {e}")
            self.stats['errors'] += 1
            return "error"

    async def _validate_token(self, token: str) -> bool:
        """Quick token validation check"""
        try:
            headers = self._get_headers(token)
            async with self.session.get(
                "https://discord.com/api/v9/users/@me",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    username = data.get('username', 'unknown')
                    discriminator = data.get('discriminator', '0')
                    nitro = data.get('premium_type', 0)
                    nitro_status = {1: "Nitro Classic", 2: "Nitro", 3: "Nitro Basic"}.get(nitro, "None")
                    logger.info(f"[VALID] {self._mask_token(token)} | User: {username}#{discriminator} | Nitro: {nitro_status}")
                    return True
                else:
                    logger.warning(f"[INVALID] {self._mask_token(token)} | Validation failed: {resp.status}")
                    return False
        except Exception as e:
            logger.warning(f"[INVALID] {self._mask_token(token)} | Validation error: {e}")
            return False

    async def run(self, validate_first: bool = False, stagger: bool = True) -> Dict:
        """Main execution flow"""
        start_time = time.time()
        self.sem = asyncio.Semaphore(self.max_concurrent)

        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        async with aiohttp.ClientSession(connector=connector) as self.session:
            # Optional validation phase
            if validate_first:
                logger.info("=== VALIDATION PHASE ===")
                valid_tokens = []
                for token in self.tokens:
                    if await self._validate_token(token):
                        valid_tokens.append(token)
                    await asyncio.sleep(random.uniform(0.5, 1.5))

                invalid_count = len(self.tokens) - len(valid_tokens)
                if invalid_count > 0:
                    logger.warning(f"Removed {invalid_count} invalid tokens")
                self.tokens = valid_tokens

            if not self.tokens:
                logger.error("No valid tokens to process!")
                return self.stats

            logger.info(f"=== BOOSTING PHASE | Tokens: {len(self.tokens)} | Guild: {self.guild_id} ===")

            # Create tasks with optional staggering
            tasks = []
            for i, token in enumerate(self.tokens):
                if stagger and i > 0:
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                task = asyncio.create_task(self._boost_single(token))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for token, result in zip(self.tokens, results):
                if isinstance(result, Exception):
                    logger.error(f"[CRITICAL] {self._mask_token(token)} | Unhandled: {result}")
                    self.stats['errors'] += 1
                    result = "critical"

                self.results.append(BoostResult(
                    token=token,
                    status=result,
                    timestamp=time.time()
                ))

        elapsed = time.time() - start_time
        self.stats['elapsed_time'] = round(elapsed, 2)
        self.stats['tokens_per_second'] = round(len(self.tokens) / elapsed, 2) if elapsed > 0 else 0

        return self.stats

    def generate_report(self) -> str:
        """Generate detailed execution report"""
        report = []
        report.append("\n" + "=" * 60)
        report.append("BOOST EXECUTION REPORT")
        report.append("=" * 60)
        report.append(f"Guild ID:        {self.guild_id}")
        report.append(f"Total Tokens:    {len(self.tokens)}")
        report.append(f"Duration:        {self.stats.get('elapsed_time', 0)}s")
        report.append(f"Throughput:      {self.stats.get('tokens_per_second', 0)} tokens/s")
        report.append("-" * 60)
        report.append(f"✅ Boosted:      {self.stats['boosted']}")
        report.append(f"⏭️  Already:      {self.stats['already_boosting']}")
        report.append(f"❌ Failed:       {self.stats['failed']}")
        report.append(f"⏳ Rate Limited: {self.stats['rate_limited']}")
        report.append(f"🔁 Retried:      {self.stats['retried']}")
        report.append(f"💥 Errors:       {self.stats['errors']}")
        report.append("=" * 60)

        if self.failed_tokens:
            report.append(f"\nFailed/Invalid Tokens: {len(self.failed_tokens)}")

        if self.rate_limits:
            report.append("\nRate Limit Buckets Hit:")
            for bucket, info in self.rate_limits.items():
                report.append(f"  - {bucket}: {info.retry_after}s")

        return "\n".join(report)

    def save_results(self, filepath: str = "boost_results.json"):
        """Save detailed results to JSON"""
        output = {
            "metadata": {
                "guild_id": self.guild_id,
                "timestamp": datetime.now().isoformat(),
                "total_tokens": len(self.tokens),
                "statistics": self.stats
            },
            "results": [
                {
                    "token_mask": self._mask_token(r.token),
                    "status": r.status,
                    "timestamp": datetime.fromtimestamp(r.timestamp).isoformat(),
                    "retries": r.retry_count,
                    "error": r.error_msg
                }
                for r in self.results
            ]
        }

        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2)

        logger.info(f"Results saved to {filepath}")


def load_tokens(filepath: str) -> List[str]:
    """Load and clean tokens from file"""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Token file not found: {filepath}")

    with open(path, 'r', encoding='utf-8') as f:
        tokens = [
            line.strip().split(':')[-1].strip()  # Handle user:pass:token format
            for line in f 
            if line.strip() and not line.startswith('#')
        ]

    # Remove duplicates
    unique = list(dict.fromkeys(tokens))
    if len(unique) < len(tokens):
        logger.info(f"Removed {len(tokens) - len(unique)} duplicate tokens")

    return unique


def load_proxies(filepath: str) -> List[str]:
    """Load proxies from file"""
    path = Path(filepath)
    if not path.exists():
        return []

    with open(path, 'r') as f:
        proxies = [line.strip() for line in f if line.strip()]

    logger.info(f"Loaded {len(proxies)} proxies")
    return proxies


def main():
    parser = argparse.ArgumentParser(
        description="Advanced Discord Guild Booster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python boost_advanced.py tokens.txt 123456789
  python boost_advanced.py tokens.txt 123456789 -c 5 --validate
  python boost_advanced.py tokens.txt 123456789 -c 10 --proxies proxies.txt
        """
    )

    parser.add_argument('tokens_file', help='File containing Discord tokens (one per line)')
    parser.add_argument('guild_id', help='Target Discord guild ID')
    parser.add_argument('-c', '--concurrent', type=int, default=3, help='Max concurrent requests (default: 3)')
    parser.add_argument('-r', '--retries', type=int, default=3, help='Max retries per token (default: 3)')
    parser.add_argument('--validate', action='store_true', help='Validate tokens before boosting')
    parser.add_argument('--proxies', type=str, help='File containing HTTP proxies')
    parser.add_argument('--no-stagger', action='store_true', help='Disable request staggering')
    parser.add_argument('--delay-min', type=float, default=1.5, help='Minimum delay between requests')
    parser.add_argument('--delay-max', type=float, default=4.0, help='Maximum delay between requests')
    parser.add_argument('--save', type=str, default='boost_results.json', help='Save results to JSON file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        tokens = load_tokens(args.tokens_file)
        if not tokens:
            logger.error("No tokens found in file!")
            sys.exit(1)

        logger.info(f"Loaded {len(tokens)} tokens")

        proxies = load_proxies(args.proxies) if args.proxies else []

        booster = DiscordBooster(
            guild_id=args.guild_id,
            tokens=tokens,
            max_concurrent=args.concurrent,
            max_retries=args.retries,
            use_proxies=bool(proxies),
            proxy_list=proxies,
            randomize_delays=True,
            delay_range=(args.delay_min, args.delay_max),
            verbose=args.verbose
        )

        asyncio.run(booster.run(
            validate_first=args.validate,
            stagger=not args.no_stagger
        ))

        print(booster.generate_report())

        if args.save:
            booster.save_results(args.save)

    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
