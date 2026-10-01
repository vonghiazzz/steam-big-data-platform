import { useEffect, useState } from 'react'
import {
  Activity,
  ArrowDownRight,
  ArrowUpRight,
  BarChart3,
  ChevronDown,
  CircleHelp,
  Clock3,
  Gamepad2,
  LayoutDashboard,
  RefreshCw,
  Search,
  Signal,
  Sparkles,
  ThumbsUp,
  Waves,
} from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import './App.css'

type View = 'overview' | 'games' | 'live'

type Game = {
  appid: number
  game_name: string
  review_count: number
  positive_reviews: number
  negative_reviews: number
  recommendation_rate: number
}

type GroupMetric = {
  genre?: string
  playtime_bucket?: string
  game_type?: string
  review_count: number
  positive_reviews: number
  negative_reviews: number
  recommendation_rate: number
  avg_playtime_hours?: number | null
}

type Review = {
  recommendationid: string
  appid: number
  voted_up: boolean | null
  playtime_at_review: number | null
  timestamp_created: string
}

type DashboardData = {
  games: Game[]
  genres: GroupMetric[]
  playtime: GroupMetric[]
  freePaid: GroupMetric[]
  reviews: Review[]
  reviewTotal: number
}

const apiBase = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '')
const chartColors = ['#b7e64b', '#57b6a5', '#fa8b68', '#88a7f2', '#d5a5e8']

const emptyData: DashboardData = {
  games: [],
  genres: [],
  playtime: [],
  freePaid: [],
  reviews: [],
  reviewTotal: 0,
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${apiBase}${path}`)
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(payload?.error?.message || `Request failed (${response.status})`)
  }
  return payload as T
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('en-US').format(value)
}

function formatPercent(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

function SteamCover({ game }: { game: Game }) {
  const [failed, setFailed] = useState(false)

  if (failed) {
    return <span className="cover-fallback" aria-hidden="true">{game.game_name.slice(0, 1)}</span>
  }

  return (
    <img
      className="game-cover"
      src={`https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/${game.appid}/header.jpg`}
      alt=""
      loading="lazy"
      onError={() => setFailed(true)}
    />
  )
}

function App() {
  const [view, setView] = useState<View>('overview')
  const [dashboard, setDashboard] = useState<DashboardData>(emptyData)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [refreshKey, setRefreshKey] = useState(0)
  const [search, setSearch] = useState('')
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)

  useEffect(() => {
    let active = true

    async function loadDashboard() {
      try {
        const [games, genres, playtime, freePaid, reviews] = await Promise.all([
          getJson<{ data: Game[] }>('/api/analytics/games'),
          getJson<{ data: GroupMetric[] }>('/api/analytics/genres'),
          getJson<{ data: GroupMetric[] }>('/api/analytics/playtime'),
          getJson<{ data: GroupMetric[] }>('/api/analytics/free-paid'),
          getJson<{ data: Review[]; total: number }>(
            '/api/realtime/reviews?page=1&page_size=6',
          ),
        ])

        if (active) {
          setDashboard({
            games: games.data,
            genres: genres.data,
            playtime: playtime.data,
            freePaid: freePaid.data,
            reviews: reviews.data,
            reviewTotal: reviews.total,
          })
          setUpdatedAt(new Date())
          setError('')
        }
      } catch (loadError) {
        if (active) {
          setError(loadError instanceof Error ? loadError.message : 'Unable to load analytics')
        }
      } finally {
        if (active) setLoading(false)
      }
    }

    void loadDashboard()
    return () => {
      active = false
    }
  }, [refreshKey])

  const totalReviews = dashboard.games.reduce((total, game) => total + game.review_count, 0)
  const positiveReviews = dashboard.games.reduce((total, game) => total + game.positive_reviews, 0)
  const negativeReviews = dashboard.games.reduce((total, game) => total + game.negative_reviews, 0)
  const recommendationRate = totalReviews ? positiveReviews / totalReviews : 0
  const sortedGames = [...dashboard.games].sort(
    (left, right) => right.recommendation_rate - left.recommendation_rate,
  )
  const visibleGames = sortedGames.filter((game) =>
    `${game.game_name} ${game.appid}`.toLowerCase().includes(search.toLowerCase()),
  )
  const chartGenres = [...dashboard.genres]
    .sort((left, right) => right.recommendation_rate - left.recommendation_rate)
    .slice(0, 7)
    .map((metric) => ({ ...metric, name: metric.genre || 'Unknown' }))
  const chartPlaytime = [...dashboard.playtime].sort(
    (left, right) => left.review_count - right.review_count,
  ).map((metric) => ({
    ...metric,
    bucketLabel: metric.playtime_bucket || 'MISSING',
  }))

  function refresh() {
    setLoading(true)
    setRefreshKey((key) => key + 1)
  }

  const navItems: { id: View; label: string; icon: typeof LayoutDashboard }[] = [
    { id: 'overview', label: 'Overview', icon: LayoutDashboard },
    { id: 'games', label: 'Games', icon: Gamepad2 },
    { id: 'live', label: 'Live feed', icon: Waves },
  ]

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="#overview" onClick={() => setView('overview')}>
          <span className="brand-mark"><Signal size={19} strokeWidth={2.6} /></span>
          <span className="brand-name">STEAM<span>/</span> SIGNAL</span>
        </a>

        <div className="workspace-label">WORKSPACE</div>
        <nav className="primary-nav" aria-label="Main navigation">
          {navItems.map(({ id, label, icon: Icon }) => (
            <button
              className={`nav-item ${view === id ? 'is-active' : ''}`}
              key={id}
              type="button"
              onClick={() => setView(id)}
            >
              <Icon size={18} />
              <span>{label}</span>
              {id === 'live' && <span className="nav-live-dot" />}
            </button>
          ))}
        </nav>

        <div className="sidebar-bottom">
          <div className="source-card">
            <div className="source-card-top"><span className="source-pulse" /> DATA SOURCE</div>
            <strong>Historical snapshot</strong>
            <span>50 games · 25k reviews</span>
          </div>
          <div className="sidebar-footer"><span>LOCAL ENVIRONMENT</span><span className="online-dot" /> CONNECTED</div>
        </div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div className="breadcrumbs"><span>Steam Analytics</span><span className="crumb-separator">/</span><strong>{navItems.find((item) => item.id === view)?.label}</strong></div>
          <div className="topbar-actions">
            <div className="api-status"><span className={error ? 'status-dot is-error' : 'status-dot'} />{error ? 'API ISSUE' : 'API CONNECTED'}</div>
            <button className="icon-button refresh-button" type="button" onClick={refresh} disabled={loading} aria-label="Refresh data" title="Refresh data">
              <RefreshCw size={16} className={loading ? 'is-spinning' : ''} />
            </button>
            <button className="help-button" type="button" aria-label="About this dashboard" title="Serving data from MongoDB via the read-only Backend API">
              <CircleHelp size={17} />
            </button>
          </div>
        </header>

        <div className="page-wrap">
          <section className="page-heading">
            <div>
              <div className="eyebrow"><span className="eyebrow-line" /> HISTORICAL ANALYTICS <span className="snapshot-tag">V1 SNAPSHOT</span></div>
              <h1>{view === 'overview' ? 'Review intelligence' : view === 'games' ? 'Game performance' : 'Realtime review feed'}</h1>
              <p>{view === 'live' ? 'New reviews from the streaming serving layer.' : 'A closer read on how players recommend the games they play.'}</p>
            </div>
            <div className="updated-label"><span>LAST SYNC</span><strong>{updatedAt ? updatedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '—'}</strong></div>
          </section>

          {error && (
            <div className="error-banner" role="alert">
              <Activity size={17} />
              <span>Could not load the complete dashboard: {error}</span>
              <button type="button" onClick={refresh}>Retry</button>
            </div>
          )}

          {view === 'overview' && (
            <>
              <section className="kpi-grid" aria-label="Snapshot summary">
                <article className="kpi-card kpi-primary">
                  <div className="kpi-top"><span>TOTAL REVIEWS</span><span className="kpi-icon"><BarChart3 size={17} /></span></div>
                  <div className="kpi-value">{loading ? '—' : formatNumber(totalReviews)}</div>
                  <div className="kpi-foot"><span className="kpi-marker" />Across the selected game sample</div>
                </article>
                <article className="kpi-card">
                  <div className="kpi-top"><span>GAMES TRACKED</span><span className="kpi-icon"><Gamepad2 size={17} /></span></div>
                  <div className="kpi-value">{loading ? '—' : formatNumber(dashboard.games.length)}</div>
                  <div className="kpi-foot">Unique Steam app IDs</div>
                </article>
                <article className="kpi-card">
                  <div className="kpi-top"><span>RECOMMENDATION RATE</span><span className="kpi-icon"><ThumbsUp size={17} /></span></div>
                  <div className="kpi-value">{loading ? '—' : formatPercent(recommendationRate)}</div>
                  <div className="kpi-foot"><span className="trend-up"><ArrowUpRight size={14} /> Positive votes</span><span> · weighted by reviews</span></div>
                </article>
                <article className="kpi-card kpi-live-card">
                  <div className="kpi-top"><span>INCREMENTAL REVIEWS</span><span className="live-mini"><span className="status-dot" /> LIVE</span></div>
                  <div className="kpi-value">{loading ? '—' : formatNumber(dashboard.reviewTotal)}</div>
                  <div className="kpi-foot">{dashboard.reviewTotal ? 'New reviews served' : 'Waiting for new events'}</div>
                </article>
              </section>

              <section className="analysis-grid">
                <article className="panel sentiment-panel">
                  <div className="panel-heading">
                    <div><span className="panel-kicker">VOTE DISTRIBUTION</span><h2>Players recommend it</h2></div>
                    <button className="select-chip" type="button" aria-label="Current scope"><span>All games</span><ChevronDown size={14} /></button>
                  </div>
                  <div className="sentiment-body">
                    <div className="donut-wrap">
                      {loading ? <div className="chart-placeholder" /> : (
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie data={[{ name: 'Recommended', value: positiveReviews }, { name: 'Not recommended', value: negativeReviews }]} dataKey="value" nameKey="name" innerRadius="72%" outerRadius="94%" paddingAngle={3} stroke="none" startAngle={90} endAngle={-270}>
                              <Cell fill="#b7e64b" />
                              <Cell fill="#e7e9e3" />
                            </Pie>
                            <Tooltip formatter={(value) => formatNumber(Number(value))} />
                          </PieChart>
                        </ResponsiveContainer>
                      )}
                      <div className="donut-center"><strong>{loading ? '—' : formatPercent(recommendationRate)}</strong><span>positive</span></div>
                    </div>
                    <div className="sentiment-stats">
                      <div className="sentiment-stat"><span className="legend-dot positive" /><div><span>Recommended</span><strong>{formatNumber(positiveReviews)}</strong></div><em>{formatPercent(recommendationRate)}</em></div>
                      <div className="sentiment-stat"><span className="legend-dot negative" /><div><span>Not recommended</span><strong>{formatNumber(negativeReviews)}</strong></div><em>{formatPercent(totalReviews ? negativeReviews / totalReviews : 0)}</em></div>
                      <div className="sentiment-note"><Sparkles size={15} /><span>Recommendation rate uses total review volume, not a simple game average.</span></div>
                    </div>
                  </div>
                </article>

                <article className="panel genre-panel">
                  <div className="panel-heading">
                    <div><span className="panel-kicker">GENRE SIGNAL</span><h2>Where players say yes</h2></div>
                    <span className="unit-label">RATE</span>
                  </div>
                  <div className="genre-chart">
                    {loading ? <div className="chart-placeholder" /> : (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={chartGenres} layout="vertical" margin={{ top: 3, right: 10, bottom: 0, left: 0 }} barCategoryGap={9}>
                          <CartesianGrid horizontal={false} stroke="#e9ece5" />
                          <XAxis type="number" domain={[0, 1]} tickFormatter={(value) => `${Math.round(value * 100)}%`} axisLine={false} tickLine={false} tick={{ fill: '#879087', fontSize: 11 }} />
                          <YAxis type="category" dataKey="name" width={92} axisLine={false} tickLine={false} tick={{ fill: '#525d54', fontSize: 11 }} />
                          <Tooltip formatter={(value) => formatPercent(Number(value))} cursor={{ fill: '#f0f3ea' }} />
                          <Bar dataKey="recommendation_rate" radius={[0, 4, 4, 0]} maxBarSize={17}>
                            {chartGenres.map((entry, index) => <Cell key={entry.name} fill={chartColors[index % chartColors.length]} />)}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                  <div className="panel-footnote">Top 7 genres by recommendation rate · genres can overlap</div>
                </article>
              </section>

              <section className="lower-grid">
                <article className="panel playtime-panel">
                  <div className="panel-heading">
                    <div><span className="panel-kicker">PLAYTIME</span><h2>Time played at review</h2></div>
                    <Clock3 size={17} className="subtle-icon" />
                  </div>
                  <div className="playtime-chart">
                    {loading ? <div className="chart-placeholder" /> : (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={chartPlaytime} margin={{ top: 14, right: 4, bottom: 0, left: -20 }} barCategoryGap={23}>
                          <CartesianGrid vertical={false} stroke="#e9ece5" />
                          <XAxis dataKey="bucketLabel" axisLine={false} tickLine={false} tick={{ fill: '#69736a', fontSize: 10 }} />
                          <YAxis axisLine={false} tickLine={false} tick={{ fill: '#879087', fontSize: 10 }} />
                          <Tooltip formatter={(value) => formatNumber(Number(value))} />
                          <Bar dataKey="review_count" radius={[4, 4, 0, 0]} maxBarSize={34}>
                            {chartPlaytime.map((entry, index) => <Cell key={entry.bucketLabel} fill={chartColors[index % chartColors.length]} />)}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                </article>

                <article className="panel recent-panel">
                  <div className="panel-heading">
                    <div><span className="panel-kicker">STREAMING LAYER</span><h2>Recent reviews</h2></div>
                    <button className="text-action" type="button" onClick={() => setView('live')}>Open feed <ArrowUpRight size={14} /></button>
                  </div>
                  {loading ? <div className="review-skeleton" /> : dashboard.reviews.length ? (
                    <div className="review-list">
                      {dashboard.reviews.slice(0, 3).map((review) => (
                        <div className="review-row" key={review.recommendationid}>
                          <span className={`review-vote ${review.voted_up ? 'is-positive' : 'is-negative'}`}>{review.voted_up ? <ThumbsUp size={14} /> : <ArrowDownRight size={14} />}</span>
                          <div className="review-copy"><strong>App {review.appid}</strong><span>{review.playtime_at_review == null ? 'Playtime unavailable' : `${(review.playtime_at_review / 60).toFixed(1)}h at review`}</span></div>
                          <time>{new Date(review.timestamp_created).toLocaleDateString()}</time>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="empty-live"><span className="empty-wave"><Waves size={19} /></span><div><strong>No new reviews yet</strong><span>Realtime events appear here when the stream sink receives them.</span></div><span className="waiting-pill"><span className="status-dot" /> WAITING</span></div>
                  )}
                </article>
              </section>

              <GameTable games={sortedGames.slice(0, 5)} compact onViewAll={() => setView('games')} />
            </>
          )}

          {view === 'games' && (
            <section className="panel games-view-panel">
              <div className="games-toolbar">
                <div><span className="panel-kicker">GAME CATALOG</span><h2>{formatNumber(dashboard.games.length)} games in snapshot</h2></div>
                <label className="search-box"><Search size={16} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search game or app ID" /><kbd>/</kbd></label>
              </div>
              <GameTable games={visibleGames} loading={loading} />
            </section>
          )}

          {view === 'live' && (
            <section className="panel live-view-panel">
              <div className="live-view-heading"><div><span className="panel-kicker">INCREMENTAL SERVING · V2</span><h2>Recent review events</h2><p>Only newly observed reviews are included; historical baseline reviews are intentionally excluded.</p></div><span className="waiting-pill"><span className="status-dot" /> {dashboard.reviewTotal ? `${formatNumber(dashboard.reviewTotal)} EVENTS` : 'WAITING FOR EVENTS'}</span></div>
              {loading ? <div className="review-skeleton large" /> : dashboard.reviews.length ? (
                <div className="review-list live-list">
                  {dashboard.reviews.map((review) => (
                    <div className="review-row" key={review.recommendationid}>
                      <span className={`review-vote ${review.voted_up ? 'is-positive' : 'is-negative'}`}>{review.voted_up ? <ThumbsUp size={14} /> : <ArrowDownRight size={14} />}</span>
                      <div className="review-copy"><strong>App {review.appid}</strong><span>Review #{review.recommendationid} · {review.playtime_at_review == null ? 'Playtime unavailable' : `${(review.playtime_at_review / 60).toFixed(1)}h at review`}</span></div>
                      <time>{new Date(review.timestamp_created).toLocaleString()}</time>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-feed"><span className="empty-wave"><Waves size={23} /></span><div><strong>The stream is ready for new reviews</strong><p>No incremental review documents are currently served. Historical analytics remain available in Overview and Games.</p></div></div>
              )}
            </section>
          )}

          <footer className="page-footer"><span>STEAM BIG DATA PLATFORM</span><span>READ-ONLY SERVING API</span><span>{updatedAt ? `UPDATED ${updatedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}` : 'AWAITING API'}</span></footer>
        </div>
      </main>
    </div>
  )
}

function GameTable({ games, compact = false, loading = false, onViewAll }: { games: Game[]; compact?: boolean; loading?: boolean; onViewAll?: () => void }) {
  return (
    <section className={`panel game-table-panel ${compact ? 'is-compact' : ''}`}>
      <div className="panel-heading table-heading">
        <div><span className="panel-kicker">GAME RANKING</span><h2>{compact ? 'Standout games' : 'All games'}</h2></div>
        {compact && <button className="text-action" type="button" onClick={onViewAll}>View all <ArrowUpRight size={14} /></button>}
      </div>
      <div className="table-scroll">
        <table>
          <thead><tr><th>GAME</th><th>REVIEW VOLUME</th><th>POSITIVE</th><th>RECOMMENDATION</th></tr></thead>
          <tbody>
            {loading ? <tr><td colSpan={4} className="table-empty">Loading game metrics…</td></tr> : games.length ? games.map((game, index) => (
              <tr key={game.appid}>
                <td><div className="game-cell"><span className="rank-number">{String(index + 1).padStart(2, '0')}</span><SteamCover game={game} /><div className="game-name"><strong>{game.game_name}</strong><span>APP {game.appid}</span></div></div></td>
                <td><strong className="volume-number">{formatNumber(game.review_count)}</strong></td>
                <td><div className="vote-cell"><span>{formatNumber(game.positive_reviews)}</span><div className="vote-track"><i style={{ width: `${game.recommendation_rate * 100}%` }} /></div></div></td>
                <td><div className="rate-cell"><strong>{formatPercent(game.recommendation_rate)}</strong><span className="rate-track"><i style={{ width: `${game.recommendation_rate * 100}%` }} /></span></div></td>
              </tr>
            )) : <tr><td colSpan={4} className="table-empty">No games match this search.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default App