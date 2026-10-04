import { useState, type FormEvent } from 'react'
import { ArrowRight, Dices, RotateCcw, Sparkles } from 'lucide-react'

type PredictionResult = {
  model_name: string
  model_run_id: string | null
  prediction: 0 | 1
  recommended: boolean
  probability_positive: number
  explanation: {
    baseline_probability: number
    baseline_description: string
    local_shap: FeatureContribution[]
    global_importance: FeatureContribution[]
    what_if_effects: WhatIfFeatureEffect[]
  }
}

type FeatureContribution = {
  attribute: string
  label: string
  value: number
}

type WhatIfFeatureEffect = {
  feature_type: 'genre' | 'category'
  value: string
  selected: boolean
  probability_after_toggle: number
  probability_delta: number
}

type Tri = 'unknown' | 'yes' | 'no'

type FormState = {
  hours: string
  price: string
  steamPurchase: Tri
  receivedForFree: Tri
  isFree: Tri
  windows: boolean
  mac: boolean
  linux: boolean
  genres: string[]
  categories: string[]
}

const apiBase = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '')

const GENRE_OPTIONS = [
  'Action', 'Adventure', 'RPG', 'Strategy', 'Simulation', 'Indie', 'Casual', 'Racing',
  'Sports', 'Free To Play', 'Massively Multiplayer', 'Early Access',
]

const CATEGORY_OPTIONS = [
  'Single-player', 'Multi-player', 'Co-op', 'Online Co-op', 'PvP', 'Online PvP',
  'Steam Achievements', 'Steam Cloud', 'Steam Trading Cards', 'Family Sharing',
  'Full controller support', 'Steam Workshop', 'In-App Purchases', 'Cross-Platform Multiplayer',
  'Remote Play Together', 'Subtitle Options', 'Captions available', 'VR Supported',
]

const EMPTY_FORM: FormState = {
  hours: '',
  price: '',
  steamPurchase: 'unknown',
  receivedForFree: 'unknown',
  isFree: 'unknown',
  windows: true,
  mac: false,
  linux: false,
  genres: [],
  categories: [],
}

function triToBool(value: Tri): boolean | null {
  return value === 'unknown' ? null : value === 'yes'
}

function formatPercent(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

function formatPercentagePoints(value: number) {
  return `${value > 0 ? '+' : ''}${(value * 100).toFixed(1)} pp`
}

function pick<T>(items: T[]): T {
  return items[Math.floor(Math.random() * items.length)]
}

function pickMany<T>(items: T[], min: number, max: number): T[] {
  const count = min + Math.floor(Math.random() * (max - min + 1))
  return [...items].sort(() => Math.random() - 0.5).slice(0, count)
}

function randomForm(): FormState {
  const isFree = Math.random() < 0.25
  const receivedForFree = !isFree && Math.random() < 0.1
  const minutes = Math.round(Math.exp(Math.random() * Math.log(30000)) + 5)
  const price = isFree ? 0 : pick([49000, 99000, 149000, 199000, 299000, 399000, 499000, 699000, 1061500])
  const genres = pickMany(GENRE_OPTIONS.filter((g) => g !== 'Free To Play'), 1, 3)
  if (isFree) genres.push('Free To Play')
  const multiplayer = Math.random() < 0.5
  return {
    hours: (minutes / 60).toFixed(1),
    price: String(price),
    steamPurchase: receivedForFree || isFree ? 'no' : 'yes',
    receivedForFree: receivedForFree ? 'yes' : 'no',
    isFree: isFree ? 'yes' : 'no',
    windows: true,
    mac: Math.random() < 0.3,
    linux: Math.random() < 0.25,
    genres,
    categories: [
      multiplayer ? 'Multi-player' : 'Single-player',
      ...pickMany(
        CATEGORY_OPTIONS.filter((c) => c !== 'Multi-player' && c !== 'Single-player'),
        2,
        5,
      ),
    ],
  }
}

export default function PredictionView() {
  const [form, setForm] = useState<FormState>(EMPTY_FORM)
  const [result, setResult] = useState<PredictionResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  function update<K extends keyof FormState>(key: K, value: FormState[K]) {
    setResult(null)
    setForm((current) => ({ ...current, [key]: value }))
  }

  function setFree(value: Tri) {
    setResult(null)
    setForm((current) => ({
      ...current,
      isFree: value,
      price: value === 'yes' ? '0' : current.price,
    }))
  }

  function toggle(key: 'genres' | 'categories', value: string) {
    setResult(null)
    setForm((current) => ({
      ...current,
      [key]: current[key].includes(value)
        ? current[key].filter((item) => item !== value)
        : [...current[key], value],
    }))
  }

  async function predict(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setLoading(true)
    setError('')
    setResult(null)

    const payload = {
      playtime_at_review: form.hours === '' ? null : Math.round(Number(form.hours) * 60),
      steam_purchase: triToBool(form.steamPurchase),
      received_for_free: triToBool(form.receivedForFree),
      is_free: triToBool(form.isFree),
      price: form.price === '' ? null : Number(form.price),
      genres: form.genres,
      categories: form.categories,
      platforms: { windows: form.windows, mac: form.mac, linux: form.linux },
      what_if_options: {
        genres: GENRE_OPTIONS,
        categories: CATEGORY_OPTIONS,
      },
    }

    try {
      const response = await fetch(`${apiBase}/api/ml/predict`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const body = await response.json()
      if (!response.ok) {
        throw new Error(body?.error?.message || `Request failed (${response.status})`)
      }
      if (
        !body?.explanation
        || !Array.isArray(body.explanation.local_shap)
        || !Array.isArray(body.explanation.global_importance)
        || !Array.isArray(body.explanation.what_if_effects)
      ) {
        throw new Error(
          'The backend is running an older prediction API. Restart it to enable model explanations.',
        )
      }
      setResult(body as PredictionResult)
    } catch (predictionError) {
      setError(
        predictionError instanceof Error
          ? predictionError.message
          : 'Unable to get a prediction',
      )
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="prediction-layout">
      <form className="panel prediction-form" onSubmit={predict}>
        <div className="panel-heading">
          <div>
            <span className="panel-kicker">REVIEW FEATURES</span>
            <h2>Describe a review</h2>
          </div>
          <div className="prediction-actions">
            <button type="button" className="prediction-ghost" onClick={() => { setForm(randomForm()); setResult(null); setError('') }}>
              <Dices size={15} /> Random
            </button>
            <button type="button" className="prediction-ghost" onClick={() => { setForm(EMPTY_FORM); setResult(null); setError('') }}>
              <RotateCcw size={15} /> Reset
            </button>
          </div>
        </div>

        <fieldset className="prediction-group">
          <legend>Player behavior</legend>
          <div className="prediction-fields">
            <label className="prediction-field">
              <span>Playtime at review <small>hours</small></span>
              <input
                type="number"
                min="0"
                step="0.1"
                value={form.hours}
                onChange={(event) => update('hours', event.target.value)}
                placeholder="e.g. 12.5"
              />
            </label>
            <TriToggle label="Purchased on Steam" value={form.steamPurchase} onChange={(v) => update('steamPurchase', v)} />
            <TriToggle label="Received for free" value={form.receivedForFree} onChange={(v) => update('receivedForFree', v)} />
          </div>
        </fieldset>

        <fieldset className="prediction-group">
          <legend>Game</legend>
          <div className="prediction-fields">
            <TriToggle label="Free-to-play" value={form.isFree} onChange={setFree} />
            <label className="prediction-field">
              <span>Price <small>VND, as in training data</small></span>
              <input
                type="number"
                min="0"
                step="1000"
                value={form.price}
                disabled={form.isFree === 'yes'}
                onChange={(event) => update('price', event.target.value)}
                placeholder="e.g. 199000"
              />
            </label>
            <div className="prediction-field">
              <span>Platforms</span>
              <div className="chip-row">
                {([['windows', 'Windows'], ['mac', 'macOS'], ['linux', 'Linux']] as const).map(([key, label]) => (
                  <button
                    type="button"
                    key={key}
                    className={`chip ${form[key] ? 'is-active' : ''}`}
                    aria-pressed={form[key]}
                    onClick={() => update(key, !form[key])}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </fieldset>

        <fieldset className="prediction-group">
          <legend>Genres <small>{form.genres.length} selected</small></legend>
          <ChipGroup options={GENRE_OPTIONS} selected={form.genres} onToggle={(v) => toggle('genres', v)} />
        </fieldset>

        <fieldset className="prediction-group">
          <legend>Categories <small>{form.categories.length} selected</small></legend>
          <ChipGroup options={CATEGORY_OPTIONS} selected={form.categories} onToggle={(v) => toggle('categories', v)} />
        </fieldset>

        {error && <div className="prediction-error" role="alert">{error}</div>}
        <button className="prediction-submit" type="submit" disabled={loading}>
          {loading ? 'Predicting + explaining…' : 'Predict recommendation'}
          {!loading && <ArrowRight size={16} />}
        </button>
      </form>

      <aside className="panel prediction-result-panel" aria-live="polite">
        <span className="panel-kicker">MODEL OUTPUT</span>
        {result ? (
          <div className="prediction-result">
            <span className={`prediction-result-mark ${result.recommended ? 'is-positive' : 'is-negative'}`}>
              {result.recommended ? 'Recommended' : 'Not recommended'}
            </span>
            <strong className="prediction-probability">{formatPercent(result.probability_positive)}</strong>
            <span className="prediction-result-label">Probability of a positive recommendation</span>
            <div
              className="prediction-probability-track"
              role="progressbar"
              aria-label="Probability of a positive recommendation"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={Math.round(result.probability_positive * 100)}
            >
              <i style={{ width: `${result.probability_positive * 100}%` }} />
            </div>
            <dl className="prediction-metadata">
              <div><dt>Model</dt><dd>{result.model_name.replaceAll('_', ' ')}</dd></div>
              <div><dt>Training run</dt><dd>{result.model_run_id || 'Configured model'}</dd></div>
            </dl>
            <section className="prediction-explanation" aria-label="Prediction explanation">
              <div className="explanation-heading">
                <strong>Why this prediction?</strong>
                <span>SHAP</span>
              </div>
              <p className="explanation-note">
                Positive values raise the positive-review probability; negative
                values lower it. Baseline: {formatPercent(result.explanation.baseline_probability)}.
                {' '}{result.explanation.baseline_description}
              </p>
              <div className="explanation-list">
                {result.explanation.local_shap.slice(0, 6).map((item) => {
                  const maxValue = Math.max(
                    ...result.explanation.local_shap.map((entry) => Math.abs(entry.value)),
                    0.0001,
                  )
                  const width = Math.abs(item.value) / maxValue * 50
                  return (
                    <div className="explanation-item" key={item.attribute}>
                      <div className="explanation-item-label">
                        <span>{item.label}</span>
                        <strong className={item.value >= 0 ? 'is-positive' : 'is-negative'}>
                          {formatPercentagePoints(item.value)}
                        </strong>
                      </div>
                      <div className="explanation-track" aria-hidden="true">
                        <i
                          className={`explanation-fill ${item.value >= 0 ? 'is-positive' : 'is-negative'}`}
                          style={{
                            left: item.value >= 0 ? '50%' : `${50 - width}%`,
                            width: `${width}%`,
                          }}
                        />
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="explanation-global">
                <div className="explanation-heading">
                  <strong>Global feature importance</strong>
                  <span>Random Forest</span>
                </div>
                <p className="explanation-note">
                  Overall model importance, not a cause-and-effect measure for this review.
                </p>
                <div className="explanation-list">
                  {result.explanation.global_importance.slice(0, 6).map((item) => {
                    const maxValue = Math.max(
                      ...result.explanation.global_importance.map((entry) => entry.value),
                      0.0001,
                    )
                    return (
                      <div className="explanation-item" key={item.attribute}>
                        <div className="explanation-item-label">
                          <span>{item.label}</span>
                          <strong>{formatPercent(item.value)}</strong>
                        </div>
                        <div className="explanation-global-track" aria-hidden="true">
                          <i style={{ width: `${item.value / maxValue * 100}%` }} />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
              <section className="explanation-what-if" aria-label="Category and genre what-if effects">
                <div className="explanation-heading">
                  <strong>Try adding or removing one at a time</strong>
                  <span>What-if</span>
                </div>
                <p className="explanation-note">
                  Keeps every other field unchanged. The delta shows how the
                  predicted positive-review probability changes for this input.
                </p>
                <WhatIfList
                  title="Categories"
                  effects={result.explanation.what_if_effects.filter((item) => item.feature_type === 'category')}
                  onToggle={(item) => toggle('categories', item.value)}
                />
                <WhatIfList
                  title="Genres"
                  effects={result.explanation.what_if_effects.filter((item) => item.feature_type === 'genre')}
                  onToggle={(item) => toggle('genres', item.value)}
                />
                <p className="explanation-note">
                  Click an option to update the form, then run the prediction again.
                </p>
              </section>
            </section>
            <p className="prediction-disclaimer">
              This is a model estimate, not a guarantee. The probability is a model
              confidence score and may not be calibrated.
            </p>
          </div>
        ) : (
          <div className="prediction-empty">
            <span className="prediction-empty-mark"><Sparkles size={20} /></span>
            <strong>Your prediction will appear here</strong>
            <p>Fill in the fields or press Random, then predict.</p>
          </div>
        )}
      </aside>
    </section>
  )
}

function WhatIfList({
  title,
  effects,
  onToggle,
}: {
  title: string
  effects: WhatIfFeatureEffect[]
  onToggle: (effect: WhatIfFeatureEffect) => void
}) {
  const strongest = effects.slice(0, 3)
  const weakest = [...effects].sort((left, right) => left.probability_delta - right.probability_delta).slice(0, 3)
  const featured = [...new Map(
    [...strongest, ...weakest].map((item) => [item.value, item]),
  ).values()]

  function effectLabel(effect: WhatIfFeatureEffect) {
    const action = effect.selected ? 'Remove' : 'Add'
    const delta = formatPercentagePoints(effect.probability_delta)
    return {
      action,
      delta,
      tone: effect.probability_delta >= 0 ? 'is-positive' : 'is-negative',
    }
  }

  function renderEffect(effect: WhatIfFeatureEffect) {
    const label = effectLabel(effect)
    return (
      <div className="what-if-item" key={effect.value}>
        <button
          type="button"
          className="what-if-action"
          aria-label={`${label.action} ${effect.value} ${title.toLowerCase()}`}
          onClick={() => onToggle(effect)}
        >
          <small>{label.action}</small> {effect.value}
        </button>
        <strong className={label.tone}>{label.delta}</strong>
      </div>
    )
  }

  return (
    <div className="what-if-group">
      <strong className="what-if-title">{title}</strong>
      <div className="what-if-list">{featured.map(renderEffect)}</div>
      {effects.length > featured.length && (
        <details className="what-if-details">
          <summary>See all {effects.length} options</summary>
          <div className="what-if-list">{effects.map(renderEffect)}</div>
        </details>
      )}
    </div>
  )
}

function TriToggle({
  label,
  value,
  onChange,
}: {
  label: string
  value: Tri
  onChange: (value: Tri) => void
}) {
  const options: [Tri, string][] = [['yes', 'Yes'], ['no', 'No'], ['unknown', '?']]
  return (
    <div className="prediction-field">
      <span>{label}</span>
      <div className="segmented" role="group" aria-label={label}>
        {options.map(([key, text]) => (
          <button
            type="button"
            key={key}
            className={value === key ? 'is-active' : ''}
            aria-pressed={value === key}
            onClick={() => onChange(key)}
          >
            {text}
          </button>
        ))}
      </div>
    </div>
  )
}

function ChipGroup({
  options,
  selected,
  onToggle,
}: {
  options: string[]
  selected: string[]
  onToggle: (value: string) => void
}) {
  return (
    <div className="chip-row">
      {options.map((option) => (
        <button
          type="button"
          key={option}
          className={`chip ${selected.includes(option) ? 'is-active' : ''}`}
          aria-pressed={selected.includes(option)}
          onClick={() => onToggle(option)}
        >
          {option}
        </button>
      ))}
    </div>
  )
}
