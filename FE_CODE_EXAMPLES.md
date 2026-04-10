# Frontend Code Examples & Templates

## 1. React Hook for Fetching Milestone Health

```javascript
// useMilestoneHealth.js
import { useState, useEffect } from 'react';

export const useMilestoneHealth = (projectId) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!projectId) return;

    const fetchData = async () => {
      try {
        setLoading(true);
        const response = await fetch(`/api/projects/${projectId}/milestone-health`);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch: ${response.statusText}`);
        }
        
        const result = await response.json();
        setData(result);
        setError(null);
      } catch (err) {
        setError(err.message);
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [projectId]);

  const refetch = async () => {
    return fetchData();
  };

  return { data, loading, error, refetch };
};
```

## 2. Week Header Component

```javascript
// WeekHeaderRow.jsx
export const WeekHeaderRow = ({ allWeeks, totalWeeks }) => {
  // Group weeks by month
  const monthGroups = {};
  
  Object.entries(allWeeks).forEach(([weekNum, weekData]) => {
    const date = new Date(weekData.start);
    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
    
    if (!monthGroups[monthKey]) {
      monthGroups[monthKey] = [];
    }
    monthGroups[monthKey].push({ weekNum: parseInt(weekNum), label: weekData.label });
  });

  return (
    <div className="week-headers">
      {/* Month row */}
      <div className="month-row">
        {Object.entries(monthGroups).map(([monthKey, weeks]) => (
          <div
            key={monthKey}
            className="month-header"
            style={{ width: `${weeks.length * 40}px` }}
          >
            {new Date(monthKey + '-01').toLocaleDateString('en-US', { 
              month: 'short', 
              year: 'numeric' 
            })}
          </div>
        ))}
      </div>
      
      {/* Week number row */}
      <div className="week-row">
        {Object.entries(allWeeks).map(([weekNum, weekData]) => (
          <div
            key={weekNum}
            className="week-header-cell"
            title={weekData.label}
          >
            {parseInt(weekNum) % 4 === 1 ? parseInt(weekNum) % 4 : ''}
          </div>
        ))}
      </div>
    </div>
  );
};
```

## 3. Practice Milestone Row

```javascript
// PracticeMilestoneRow.jsx
import { useState } from 'react';

export const PracticeMilestoneRow = ({ 
  milestone, 
  allWeeks, 
  onStatusChange 
}) => {
  const [hoveredWeek, setHoveredWeek] = useState(null);

  const getWeekCell = (weekNum) => {
    const week = milestone.weeks.find(w => w.week_number === weekNum);
    
    // ✅ NEW: Even empty cells are now clickable and editable
    const isEmpty = !week;
    
    return (
      <div
        key={`${milestone.milestone_code}-week-${weekNum}`}
        className={`practice-cell ${isEmpty ? 'empty-cell' : ''}`}
        style={{
          backgroundColor: isEmpty ? '#f3f4f6' : (
            week.color === 'green' ? '#22c55e' :
            week.color === 'blue' ? '#3b82f6' :
            week.color === 'orange' ? '#f97316' :
            week.color === 'red' ? '#ef4444' :
            '#d1d5db'
          ),
          cursor: 'pointer',
          border: isEmpty ? '1px dashed #d1d5db' : 'none',
          opacity: isEmpty ? 0.6 : 1,
          transition: 'all 0.2s ease'
        }}
        onMouseEnter={() => setHoveredWeek(weekNum)}
        onMouseLeave={() => setHoveredWeek(null)}
        onClick={() => onStatusChange({
          type: 'practice',
          milestoneId: milestone.id,
          milestoneCode: milestone.milestone_code,
          weekNumber: weekNum,
          weekLabel: allWeeks[weekNum].label,
          currentStatus: week?.status || null,
          isEmpty: isEmpty,
          milestone
        })}
        title={isEmpty ? `${allWeeks[weekNum].label} - Click to add status` : `${allWeeks[weekNum].label} - ${week.status}`}
        role="button"
        tabIndex={0}
        aria-label={isEmpty 
          ? `Week ${weekNum}, ${allWeeks[weekNum].label}. Click to add status for ${milestone.milestone_code}.`
          : `${milestone.milestone_code} Week ${weekNum}, ${allWeeks[weekNum].label}, ${week.status}. Press to edit.`
        }
      >
        {week && hoveredWeek === weekNum && (
          <span className="status-badge">{week.status[0]}</span>
        )}
        {isEmpty && hoveredWeek === weekNum && (
          <span className="add-badge">+</span>
        )}
      </div>
    );
  };

  return (
    <div className="milestone-row practice-row">
      <div className="milestone-label">
        <div className="code">{milestone.milestone_code}</div>
        <div className="description">{milestone.description}</div>
      </div>
      
      <div className="weeks-container">
        {Object.keys(allWeeks).map(weekNum => getWeekCell(parseInt(weekNum)))}
      </div>
      
      <div className="milestone-stats">
        <span className="completion">{milestone.completion_pct}%</span>
        <span className="variance">{milestone.days_variance}d</span>
      </div>
    </div>
  );
};
```

## 4. Signoff Marker Row

```javascript
// SignoffMilestoneRow.jsx
export const SignoffMilestoneRow = ({ 
  milestone, 
  allWeeks,
  onStatusChange 
}) => {
  const getMarkerCell = (weekNum) => {
    if (milestone.weeks.length === 0) return null;
    if (milestone.weeks[0].week_number !== weekNum) return null;

    const week = milestone.weeks[0];
    const isDone = milestone.signoff_status?.toLowerCase() === 'done';
    
    return (
      <div
        key={`${milestone.milestone_code}-signoff-${weekNum}`}
        className="signoff-cell"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer'
        }}
        onClick={() => onStatusChange({
          type: 'signoff',
          milestoneId: milestone.id,
          milestone
        })}
        title={`${week.milestone_code}: ${week.status} on ${week.date}`}
      >
        <div
          className="marker circle"
          style={{
            width: '28px',
            height: '28px',
            borderRadius: '50%',
            backgroundColor: isDone ? '#22c55e' : 'transparent',
            border: isDone ? '2px solid #22c55e' : '2px solid #f97316',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            position: 'relative'
          }}
        >
          <span 
            style={{ 
              color: isDone ? 'white' : '#f97316', 
              fontSize: '10px',
              fontWeight: '700',
              textAlign: 'center',
              lineHeight: '1'
            }}
          >
            {week.milestone_code}
          </span>
        </div>
      </div>
    );
  };

  return (
    <div className="milestone-row signoff-row">
      <div className="milestone-label">
        <div className="code">{milestone.milestone_code}</div>
        <div className="description">{milestone.description}</div>
      </div>
      
      <div className="weeks-container">
        {Object.keys(allWeeks).map(weekNum => getMarkerCell(parseInt(weekNum)))}
      </div>
    </div>
  );
};
```

## 5. Invoice Marker Row

```javascript
// InvoiceMilestoneRow.jsx
export const InvoiceMilestoneRow = ({ 
  milestone, 
  allWeeks,
  onStatusChange 
}) => {
  const getMarkerCell = (weekNum) => {
    if (milestone.weeks.length === 0) return null;
    if (milestone.weeks[0].week_number !== weekNum) return null;

    const week = milestone.weeks[0];
    const isDone = milestone.invoice_status?.toLowerCase() === 'done';
    
    return (
      <div
        key={`${milestone.milestone_code}-invoice-${weekNum}`}
        className="invoice-cell"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer'
        }}
        onClick={() => onStatusChange({
          type: 'invoice',
          milestoneId: milestone.id,
          milestone
        })}
        title={`${week.milestone_code}: Invoice ${week.status} on ${week.date}`}
      >
        <div
          className="marker diamond"
          style={{
            width: '28px',
            height: '28px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            position: 'relative'
          }}
        >
          {/* Diamond shape */}
          <div
            style={{
              width: '20px',
              height: '20px',
              transform: 'rotate(45deg)',
              backgroundColor: isDone ? '#22c55e' : 'transparent',
              border: isDone ? '2px solid #22c55e' : '2px solid #f97316',
              position: 'absolute'
            }}
          />
          {/* Milestone code text */}
          <span 
            style={{ 
              color: isDone ? 'white' : '#f97316', 
              fontSize: '10px',
              fontWeight: '700',
              transform: 'rotate(-45deg)',
              position: 'relative',
              zIndex: 1,
              lineHeight: '1'
            }}
          >
            {week.milestone_code}
          </span>
        </div>
      </div>
    );
  };

  return (
    <div className="milestone-row invoice-row">
      <div className="milestone-label">
        <div className="code">{milestone.milestone_code}</div>
        <div className="description">{milestone.description}</div>
      </div>
      
      <div className="weeks-container">
        {Object.keys(allWeeks).map(weekNum => getMarkerCell(parseInt(weekNum)))}
      </div>
    </div>
  );
};
```

## 6. Status Editor Modal

```javascript
// StatusEditorModal.jsx
import { useState } from 'react';

export const StatusEditorModal = ({ 
  milestone,
  weekNumber,
  weekLabel,
  isEmpty, // ✅ NEW: Is this an empty cell?
  type, // 'practice' | 'signoff' | 'invoice'
  isOpen,
  onClose,
  onSave 
}) => {
  // ✅ NEW: Handle empty cells (default to first option)
  const [selectedStatus, setSelectedStatus] = useState(
    isEmpty ? 
      (type === 'practice' ? 'On Track' : 'Pending') :
      (type === 'practice' ? milestone.status :
       type === 'signoff' ? milestone.signoff_status :
       type === 'invoice' ? milestone.invoice_status :
       'Pending')
  );
  
  const [selectedDate, setSelectedDate] = useState(
    type === 'signoff' ? milestone.signedoff_date :
    type === 'invoice' ? milestone.invoice_raised_date :
    new Date().toISOString().split('T')[0]
  );

  const handleSave = async () => {
    const updates = {};
    
    if (type === 'practice') {
      updates.status = selectedStatus;
    } else if (type === 'signoff') {
      updates.client_signoff_status = selectedStatus;
      if (selectedStatus === 'Done') {
        updates.signedoff_date = selectedDate;
      }
    } else if (type === 'invoice') {
      updates.invoice_status = selectedStatus;
      if (selectedStatus === 'Done') {
        updates.invoice_raised_date = selectedDate;
      }
    }

    try {
      await onSave(milestone.id, updates);
      onClose();
    } catch (err) {
      console.error('Save failed:', err);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>
            {isEmpty ? 'Add Status' : 'Update'} {milestone.milestone_code}
          </h3>
          <p style={{ margin: '4px 0 0 0', fontSize: '12px', color: '#6b7280' }}>
            Week: {weekLabel}
          </p>
          <button onClick={onClose}>×</button>
        </div>

        <div className="modal-body">
          {/* Status Selector */}
          <div className="form-group">
            <label>Status</label>
            <select 
              value={selectedStatus} 
              onChange={(e) => setSelectedStatus(e.target.value)}
            >
              {type === 'practice' && (
                <>
                  <option value="On Track">On Track</option>
                  <option value="At Risk">At Risk</option>
                  <option value="Blocked">Blocked</option>
                  <option value="Completed">Completed</option>
                </>
              )}
              {(type === 'signoff' || type === 'invoice') && (
                <>
                  <option value="Pending">Pending</option>
                  <option value="Done">Done</option>
                </>
              )}
            </select>
          </div>

          {/* Date Selector (for signoff/invoice when Done) */}
          {(type === 'signoff' || type === 'invoice') && 
           selectedStatus === 'Done' && (
            <div className="form-group">
              <label>
                {type === 'signoff' ? 'Signoff Date' : 'Invoice Date'}
              </label>
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                max={new Date().toISOString().split('T')[0]}
              />
            </div>
          )}

          {/* Info Text */}
          <div className="modal-info">
            <p>
              {type === 'practice' && 'Update the milestone status.'}
              {type === 'signoff' && 'Mark as Done when client has signed off.'}
              {type === 'invoice' && 'Mark as Done when invoice has been raised.'}
            </p>
          </div>
        </div>

        <div className="modal-footer">
          <button onClick={onClose} className="btn-secondary">Cancel</button>
          <button onClick={handleSave} className="btn-primary">Save</button>
        </div>
      </div>
    </div>
  );
};
```

## 7. Main Tracker Component

```javascript
// MilestoneHealthTracker.jsx
import { useState } from 'react';
import { useMilestoneHealth } from './useMilestoneHealth';
import { WeekHeaderRow } from './WeekHeaderRow';
import { PracticeMilestoneRow } from './PracticeMilestoneRow';
import { SignoffMilestoneRow } from './SignoffMilestoneRow';
import { InvoiceMilestoneRow } from './InvoiceMilestoneRow';
import { StatusEditorModal } from './StatusEditorModal';

export const MilestoneHealthTracker = ({ projectId }) => {
  const { data, loading, error } = useMilestoneHealth(projectId);
  const [modalData, setModalData] = useState(null);
  // ✅ NEW: Store additional modal context
  const [modalContext, setModalContext] = useState({});

  // ✅ NEW: Enhanced handler for both filled and empty cells
  const handleStatusChange = (changeData) => {
    setModalData(changeData);
    setModalContext({
      weekNumber: changeData.weekNumber,
      weekLabel: changeData.weekLabel,
      isEmpty: changeData.isEmpty || false,
      milestoneCode: changeData.milestoneCode
    });
  };

  const handleSave = async (milestoneId, updates) => {
    try {
      const response = await fetch(`/api/milestones/${milestoneId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });

      if (!response.ok) throw new Error('Update failed');
      
      // Refresh data
      window.location.reload(); // or call refetch() if available
    } catch (err) {
      alert(`Error updating milestone: ${err.message}`);
    }
  };

  if (loading) return <div className="loading">Loading milestone health...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!data) return <div className="error">No data available</div>;

  return (
    <div className="milestone-health-tracker">
      <div className="tracker-header">
        <h2>{data.project_name} - Milestone Health</h2>
        <p>Last Updated: {new Date().toLocaleDateString()}</p>
      </div>

      <div className="tracker-container">
        {/* Week Headers */}
        <WeekHeaderRow 
          allWeeks={data.all_weeks}
          totalWeeks={data.weeks_range.total_weeks}
        />

        {/* Practice Section */}
        <div className="section practice-section">
          <div className="section-title">PRACTICE</div>
          {data.practice.map(milestone => (
            <PracticeMilestoneRow
              key={milestone.id}
              milestone={milestone}
              allWeeks={data.all_weeks}
              onStatusChange={handleStatusChange}
            />
          ))}
        </div>

        {/* Signoff Section */}
        <div className="section signoff-section">
          <div className="section-title">CLIENT SIGNOFF</div>
          {data.signoff.map(milestone => (
            <SignoffMilestoneRow
              key={milestone.id}
              milestone={milestone}
              allWeeks={data.all_weeks}
              onStatusChange={handleStatusChange}
            />
          ))}
        </div>

        {/* Invoice Section */}
        <div className="section invoice-section">
          <div className="section-title">INVOICE HEALTH</div>
          {data.invoice.map(milestone => (
            <InvoiceMilestoneRow
              key={milestone.id}
              milestone={milestone}
              allWeeks={data.all_weeks}
              onStatusChange={handleStatusChange}
            />
          ))}
        </div>
      </div>

      {/* Status Editor Modal */}
      {modalData && (
        <StatusEditorModal
          milestone={modalData.milestone}
          type={modalData.type}
          weekNumber={modalContext.weekNumber}
          weekLabel={modalContext.weekLabel}
          isEmpty={modalContext.isEmpty}
          isOpen={!!modalData}
          onClose={() => {
            setModalData(null);
            setModalContext({});
          }}
          onSave={handleSave}
        />
      )}
    </div>
  );
};
```

## 8. CSS Styling

```css
/* MilestoneHealthTracker.css */

.milestone-health-tracker {
  padding: 20px;
  background: #f9fafb;
  border-radius: 8px;
}

.tracker-header {
  margin-bottom: 30px;
}

.tracker-header h2 {
  font-size: 24px;
  font-weight: 700;
  color: #1f2937;
  margin: 0 0 8px 0;
}

.tracker-header p {
  font-size: 12px;
  color: #6b7280;
  margin: 0;
}

.tracker-container {
  overflow-x: auto;
  background: white;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
}

.week-headers {
  display: grid;
  grid-template-columns: 180px repeat(auto-fit, minmax(40px, 1fr));
  background: #f3f4f6;
  border-bottom: 2px solid #e5e7eb;
  sticky top: 0;
  z-index: 10;
}

.month-row {
  display: grid;
  grid-column: 2 / -1;
  grid-template-columns: repeat(auto-fit, minmax(40px, 1fr));
}

.month-header {
  padding: 8px 4px;
  font-weight: 600;
  font-size: 12px;
  text-align: center;
  border-right: 1px solid #d1d5db;
  color: #374151;
}

.week-row {
  display: grid;
  grid-column: 2 / -1;
  grid-template-columns: repeat(auto-fit, minmax(40px, 1fr));
}

.week-header-cell {
  padding: 6px 4px;
  text-align: center;
  font-size: 11px;
  font-weight: 500;
  color: #6b7280;
  border-right: 1px solid #e5e7eb;
}

.section {
  border-top: 2px solid #e5e7eb;
}

.section-title {
  padding: 12px 16px;
  font-weight: 700;
  font-size: 13px;
  color: #374151;
  background: #f9fafb;
  border-bottom: 1px solid #e5e7eb;
}

.milestone-row {
  display: grid;
  grid-template-columns: 180px 1fr 80px;
  border-bottom: 1px solid #f3f4f6;
  align-items: center;
}

.milestone-label {
  padding: 12px 16px;
  border-right: 1px solid #e5e7eb;
}

.milestone-label .code {
  font-weight: 600;
  font-size: 13px;
  color: #1f2937;
}

.milestone-label .description {
  font-size: 12px;
  color: #6b7280;
  margin-top: 2px;
}

.weeks-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(40px, 1fr));
  gap: 1px;
  padding: 8px;
  background: #f9fafb;
}

.practice-cell {
  aspect-ratio: 1;
  border-radius: 4px;
  transition: all 0.2s ease;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
}

.practice-cell:hover {
  transform: scale(1.15);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
  z-index: 20;
}

/* ✅ NEW: Empty cell styling */
.practice-cell.empty-cell {
  background-color: #f3f4f6 !important;
  border: 2px dashed #d1d5db;
  opacity: 0.6;
}

.practice-cell.empty-cell:hover {
  opacity: 1;
  border-color: #9ca3af;
  background-color: #e5e7eb !important;
  box-shadow: inset 0 0 0 1px #9ca3af;
}

.status-badge,
.add-badge {
  font-size: 10px;
  font-weight: 700;
  color: white;
}

.add-badge {
  color: #9ca3af;
  font-size: 14px;
}

.status-badge {
  font-size: 10px;
  font-weight: 700;
  color: white;
}

.signoff-cell,
.invoice-cell {
  aspect-ratio: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  padding: 4px;
}

.marker {
  transition: all 0.2s ease;
}

.marker:hover {
  transform: scale(1.3);
}

.milestone-stats {
  padding: 12px 16px;
  text-align: right;
  font-size: 12px;
  color: #6b7280;
  border-left: 1px solid #e5e7eb;
  display: flex;
  gap: 12px;
  justify-content: flex-end;
}

.completion {
  font-weight: 600;
  color: #1f2937;
}

/* Modal Styles */
.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-content {
  background: white;
  border-radius: 8px;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
  width: 90%;
  max-width: 400px;
}

.modal-header {
  padding: 16px;
  border-bottom: 1px solid #e5e7eb;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.modal-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #1f2937;
}

.modal-header button {
  background: none;
  border: none;
  font-size: 24px;
  color: #6b7280;
  cursor: pointer;
}

.modal-body {
  padding: 20px;
}

.form-group {
  margin-bottom: 16px;
}

.form-group label {
  display: block;
  font-size: 12px;
  font-weight: 600;
  color: #374151;
  margin-bottom: 6px;
}

.form-group select,
.form-group input {
  width: 100%;
  padding: 8px 12px;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  font-size: 14px;
}

.modal-info {
  padding: 12px;
  background: #f3f4f6;
  border-radius: 4px;
  margin-top: 16px;
}

.modal-info p {
  margin: 0;
  font-size: 12px;
  color: #6b7280;
}

.modal-footer {
  padding: 16px;
  border-top: 1px solid #e5e7eb;
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}

.btn-primary,
.btn-secondary {
  padding: 8px 16px;
  border-radius: 4px;
  border: none;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-primary {
  background: #3b82f6;
  color: white;
}

.btn-primary:hover {
  background: #2563eb;
}

.btn-secondary {
  background: #e5e7eb;
  color: #374151;
}

.btn-secondary:hover {
  background: #d1d5db;
}

.loading,
.error {
  padding: 20px;
  text-align: center;
  font-size: 14px;
  color: #6b7280;
}

.error {
  color: #dc2626;
  background: #fee2e2;
  border-radius: 4px;
}
```

---

## Usage Example

```javascript
// In your page or component
import { MilestoneHealthTracker } from './MilestoneHealthTracker';

export default function ProjectDashboard() {
  const projectId = 'c1000000-0000-0000-0000-000000000001';

  return (
    <main>
      <MilestoneHealthTracker projectId={projectId} />
    </main>
  );
}
```

This gives your frontend developer everything needed to build the UI!
