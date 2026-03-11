import { useEffect, useState } from 'react'
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

export interface LabOption {
  id: string
  title: string
}

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number
  attempts: number
}

interface DashboardProps {
  token: string
  labs: LabOption[]
}

type DashboardState =
  | { status: 'loading' }
  | {
      status: 'success'
      scores: ScoreBucket[]
      timeline: TimelinePoint[]
      passRates: PassRateRow[]
    }
  | { status: 'error'; message: string }

async function fetchJson<T>(path: string, token: string): Promise<T> {
  const response = await fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }
  return (await response.json()) as T
}

function formatLabId(title: string): string | null {
  const match = title.match(/Lab\s+(\d+)/i)
  if (!match) return null
  return `lab-${match[1].padStart(2, '0')}`
}

export function extractLabs(items: Array<{ type: string; title: string }>): LabOption[] {
  const seen = new Set<string>()
  const labs: LabOption[] = []

  for (const item of items) {
    if (item.type !== 'lab') continue
    const id = formatLabId(item.title)
    if (!id || seen.has(id)) continue
    seen.add(id)
    labs.push({ id, title: item.title })
  }

  return labs.sort((left, right) => left.id.localeCompare(right.id))
}

function Dashboard({ token, labs }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>(labs[0]?.id ?? 'lab-04')
  const [state, setState] = useState<DashboardState>({ status: 'loading' })

  useEffect(() => {
    if (!labs.some((lab) => lab.id === selectedLab) && labs[0]) {
      setSelectedLab(labs[0].id)
    }
  }, [labs, selectedLab])

  useEffect(() => {
    let cancelled = false
    const activeLab = labs.some((lab) => lab.id === selectedLab)
      ? selectedLab
      : (labs[0]?.id ?? selectedLab)

    setState({ status: 'loading' })

    void Promise.all([
      fetchJson<ScoreBucket[]>(`/analytics/scores?lab=${activeLab}`, token),
      fetchJson<TimelinePoint[]>(`/analytics/timeline?lab=${activeLab}`, token),
      fetchJson<PassRateRow[]>(`/analytics/pass-rates?lab=${activeLab}`, token),
    ])
      .then(([scores, timeline, passRates]) => {
        if (cancelled) return
        setState({ status: 'success', scores, timeline, passRates })
      })
      .catch((error: unknown) => {
        if (cancelled) return
        setState({
          status: 'error',
          message: error instanceof Error ? error.message : 'Unknown error',
        })
      })

    return () => {
      cancelled = true
    }
  }, [labs, selectedLab, token])

  const labChoices = labs.length > 0 ? labs : [{ id: selectedLab, title: selectedLab }]

  return (
    <section className="dashboard">
      <div className="dashboard-toolbar">
        <div>
          <h2>Dashboard</h2>
          <p>Visualize score buckets, submission activity, and task performance.</p>
        </div>
        <label className="lab-select">
          <span>Lab</span>
          <select
            value={selectedLab}
            onChange={(event) => setSelectedLab(event.target.value)}
          >
            {labChoices.map((lab) => (
              <option key={lab.id} value={lab.id}>
                {lab.title}
              </option>
            ))}
          </select>
        </label>
      </div>

      {state.status === 'loading' && <p className="status-card">Loading dashboard...</p>}
      {state.status === 'error' && (
        <p className="status-card status-card-error">Error: {state.message}</p>
      )}

      {state.status === 'success' && (
        <div className="dashboard-grid">
          <section className="panel chart-panel">
            <h3>Score Distribution</h3>
            <Bar
              data={{
                labels: state.scores.map((entry) => entry.bucket),
                datasets: [
                  {
                    label: 'Submissions',
                    data: state.scores.map((entry) => entry.count),
                    backgroundColor: ['#0f766e', '#0ea5e9', '#f59e0b', '#ef4444'],
                    borderRadius: 8,
                  },
                ],
              }}
              options={{
                responsive: true,
                plugins: { legend: { display: false } },
              }}
            />
          </section>

          <section className="panel chart-panel">
            <h3>Submission Timeline</h3>
            <Line
              data={{
                labels: state.timeline.map((entry) => entry.date),
                datasets: [
                  {
                    label: 'Submissions',
                    data: state.timeline.map((entry) => entry.submissions),
                    borderColor: '#0f172a',
                    backgroundColor: 'rgba(14, 165, 233, 0.18)',
                    tension: 0.3,
                    fill: true,
                  },
                ],
              }}
              options={{
                responsive: true,
                plugins: { legend: { display: false } },
              }}
            />
          </section>

          <section className="panel table-panel">
            <div className="panel-header">
              <h3>Pass Rates</h3>
              <span>{state.passRates.length} tasks</span>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Avg score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {state.passRates.map((row) => (
                  <tr key={row.task}>
                    <td>{row.task}</td>
                    <td>{row.avg_score.toFixed(1)}</td>
                    <td>{row.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </div>
      )}
    </section>
  )
}

export default Dashboard
