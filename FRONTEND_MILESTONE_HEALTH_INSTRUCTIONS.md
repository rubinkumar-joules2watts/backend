# Frontend System Instructions: Milestone Health Tracker UI

## Overview
Implement a comprehensive milestone health tracker that displays practice, signoff, and invoice milestones with week-wise progress visualization, similar to GitHub's contribution graph.

---

## API Endpoint Integration

### Endpoint Details
- **URL**: `GET /projects/{project_id}/milestone-health`
- **Method**: GET
- **Path Parameter**: `project_id` (string)
- **Response**: Milestone health data with weeks visualization

### API Response Structure
```json
{
  "project_id": "string",
  "project_name": "string",
  "practice": [
    {
      "id": "8018f24b-0701-47f2-b566-c25cf0513fdf",
      "milestone_code": "M1",
      "description": "Verification and SOW",
      "milestone_type": "practice",
      "start_date": "2025-02-02T00:00:00+00:00",
      "end_date": "2025-02-14T00:00:00+00:00",
      "weeks": [
        {
          "week_number": 0,
          "week_label": "Jan 27, 2025",
          "status": "On Track",  // "On Track", "Completed", "At Risk", "Blocked"
          "color": "green",       // "green", "blue", "orange", "red", "gray"
          "date": "2025-01-27T00:00:00+00:00"
        }
      ],
      "completion_pct": 100,
      "status": "Completed",
      "color": "blue",
      "days_variance": 28
    }
  ],
  "signoff": [...],  // Same structure as practice (with id)
  "invoice": [...],  // Same structure as practice (with id)
  "weeks_range": {
    "start_week": "Jan 27, 2025",
    "end_week": "Apr 6, 2026",
    "total_weeks": 63
  },
  "all_weeks": {
    "0": { "label": "Jan 27, 2025", "start": "2025-01-27T00:00:00+00:00" },
    "1": { "label": "Feb 3, 2025", "start": "2025-02-03T00:00:00+00:00" },
    // ... continues for all weeks
  }
}
```

---

## UI Layout Structure

### 1. Header Section
```
┌─────────────────────────────────────────────────────────────┐
│ ITSE Dashboard - Milestone Health                           │
│ Last Updated: Apr 10, 2026 at 3:35 PM                       │
└─────────────────────────────────────────────────────────────┘
```

### 2. Week Headers (Horizontal Timeline)
Display the weeks as column headers showing:
- Month name (Jan, Feb, Mar, etc.)
- Week numbers (1, 2, 3, 4)
- Each column represents a week in `all_weeks`

Example layout:
```
       Jan                 Feb                 Mar        Apr
    1  2  3  4      1  2  3  4      1  2  3  4    1  2
```

### 3. Milestone Rows (Three sections)

#### PRACTICE Section
- **Row Label**: "PRACTICE"
- **Legend**: Green circle (On Track), Blue (Completed), Orange (At Risk), Red (Blocked), Gray (Not Started)
- **Display**: Show milestone code (M1, M2, M3) with color-coded cells for each week
- **Interactivity**: Each cell is clickable for status updates

#### SIGNOFF Section
- **Row Label**: "CLIENT SIGNOFF"
- **Legend**: Green circle (Done), Orange circle (Pending)
- **Display**: Show milestone code with a single marker (circle) in the week where signoff occurred
- **Note**: Shows actual signoff date if status="Done", otherwise shows at actual_end_eta

#### INVOICE Section
- **Row Label**: "INVOICE HEALTH"
- **Legend**: Green diamond (Done), Orange diamond (Pending)
- **Display**: Show milestone code with a single marker (diamond) in the week where invoice was raised
- **Note**: Shows actual invoice_raised_date if status="Done", otherwise shows at actual_end_eta

---

## Data Mapping Guide

### Color Mapping (Practice Milestones)
```javascript
const statusColorMap = {
  "On Track": {
    backgroundColor: "#22c55e",  // green-500
    borderColor: "#16a34a",      // green-600
    statusText: "On Track"
  },
  "Completed": {
    backgroundColor: "#3b82f6",  // blue-500
    borderColor: "#1d4ed8",      // blue-600
    statusText: "Completed"
  },
  "At Risk": {
    backgroundColor: "#f97316",  // orange-500
    borderColor: "#ea580c",      // orange-600
    statusText: "At Risk"
  },
  "Blocked": {
    backgroundColor: "#ef4444",  // red-500
    borderColor: "#dc2626",      // red-600
    statusText: "Blocked"
  },
  "Not Started": {
    backgroundColor: "#d1d5db",  // gray-300
    borderColor: "#9ca3af",      // gray-400
    statusText: "Not Started"
  }
};
```

### Signoff Status Mapping
```javascript
const signoffStatusMap = {
  "Done": {
    icon: "●",            // filled circle
    color: "#22c55e",     // green
    backgroundColor: "rgba(34, 197, 94, 0.1)"
  },
  "Pending": {
    icon: "○",            // empty circle
    color: "#f97316",     // orange
    backgroundColor: "rgba(249, 115, 22, 0.1)"
  }
};
```

### Invoice Status Mapping
```javascript
const invoiceStatusMap = {
  "Done": {
    icon: "◆",            // filled diamond
    color: "#22c55e",     // green
    backgroundColor: "rgba(34, 197, 94, 0.1)"
  },
  "Pending": {
    icon: "◇",            // empty diamond
    color: "#f97316",     // orange
    backgroundColor: "rgba(249, 115, 22, 0.1)"
  }
};
```

---

## Component Implementation Details

### 1. Week Headers Component
- **Responsibility**: Render month names and week numbers in columns
- **Props**: 
  - `allWeeks`: Object from API response containing all weeks
  - `weekNumbers`: Array of week numbers to display
- **Logic**: Group weeks by month, display month header above, then week numbers below

### 2. Practice Milestone Row Component
- **Responsibility**: Render practice milestone progress across weeks
- **Props**:
  - `milestone`: Single milestone object from `practice` array
  - `allWeeks`: All weeks data
  - `onStatusChange`: Callback when cell is edited
- **Features**:
  - Show milestone code and description
  - Display colored cells only for weeks in `milestone.weeks`
  - Show completion percentage and days variance
  - Tooltip on hover showing: `{week_label} - {status} - {actual date}`

### 3. Signoff/Invoice Marker Component
- **Responsibility**: Render point-in-time markers (circles/diamonds)
- **Props**:
  - `milestone`: Single milestone object
  - `markerType`: "signoff" or "invoice"
  - `allWeeks`: All weeks data
  - `onStatusChange`: Callback for status updates
- **Features**:
  - Display only one marker in the appropriate week
  - Show appropriate icon (circle for signoff, diamond for invoice)
  - Tooltip showing actual date of completion/pending

### 4. Status Editor Modal
- **Trigger**: Click on any cell/marker in the tracker
- **Fields to Update**:
  - For Practice: `status` (On Track, At Risk, Blocked, Completed)
  - For Signoff: `client_signoff_status` (Pending, Done) + optional `signedoff_date`
  - For Invoice: `invoice_status` (Pending, Done) + optional `invoice_raised_date`
- **API Call**: PATCH `/milestones/{milestone_id}` with updated fields
- **Validation**: 
  - If status=Done, require a date
  - Date must be on or before current date
  - Cannot change historical data (warn user)

---

## Week Cell Interactivity

### Click Handler for Practice Cells
```javascript
const handlePracticeCellClick = (milestone, weekNumber) => {
  // Open modal with current status
  openStatusEditorModal({
    milestoneId: milestone.id,
    milestoneCode: milestone.milestone_code,
    type: "practice",
    currentStatus: milestone.status,
    weekNumber: weekNumber,
    weekLabel: all_weeks[weekNumber].label,
    onSave: (newStatus) => updateMilestoneStatus(...)
  });
};
```

### Click Handler for Signoff Marker
```javascript
const handleSignoffMarkerClick = (milestone) => {
  // Open modal with signoff-specific options
  openStatusEditorModal({
    milestoneId: milestone.id,
    milestoneCode: milestone.milestone_code,
    type: "signoff",
    currentStatus: milestone.signoff_status,
    currentDate: milestone.date,
    onSave: (newStatus, newDate) => updateSignoffStatus(...)
  });
};
```

### Click Handler for Invoice Marker
```javascript
const handleInvoiceMarkerClick = (milestone) => {
  // Open modal with invoice-specific options
  openStatusEditorModal({
    milestoneId: milestone.id,
    milestoneCode: milestone.milestone_code,
    type: "invoice",
    currentStatus: milestone.invoice_status,
    currentDate: milestone.date,
    onSave: (newStatus, newDate) => updateInvoiceStatus(...)
  });
};
```

---

## Data Fetching & State Management

### Fetch Milestone Health Data
```javascript
// In your component (React/Next.js)
const [milestoneData, setMilestoneData] = useState(null);
const [loading, setLoading] = useState(true);
const [error, setError] = useState(null);

useEffect(() => {
  const fetchMilestoneHealth = async () => {
    try {
      const response = await fetch(`/api/projects/${projectId}/milestone-health`);
      if (!response.ok) throw new Error("Failed to fetch milestone health");
      const data = await response.json();
      setMilestoneData(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  fetchMilestoneHealth();
}, [projectId]);
```

### Update Milestone Status
```javascript
const updateMilestoneStatus = async (milestoneId, updatePayload) => {
  try {
    const response = await fetch(`/api/milestones/${milestoneId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(updatePayload)
    });
    
    if (!response.ok) throw new Error("Failed to update milestone");
    
    // Refresh milestone health data
    await fetchMilestoneHealth();
  } catch (err) {
    console.error("Update failed:", err);
    // Show error toast to user
  }
};
```

---

## Display Logic Rules

### Practice Milestones
- **Show cells for**: All weeks in `milestone.weeks` array
- **Cell content**: Colored square with status indicator
- **Week range**: From `start_date` week to `end_date` week
- **Status color**: Based on `milestone.weeks[i].color`
- **Tooltip**: `{week_label} - {status} - {completion: XX%}`

### Signoff Milestone
- **Show marker in**: Week calculated from `date` field
- **If status=Pending**: Show at `actual_end_eta` week with orange circle
- **If status=Done**: Show at `signedoff_date` week with green circle
- **Tooltip**: `Signoff: {status} on {date}`

### Invoice Milestone
- **Show marker in**: Week calculated from `date` field
- **If status=Pending**: Show at `actual_end_eta` week with orange diamond
- **If status=Done**: Show at `invoice_raised_date` week with green diamond
- **Tooltip**: `Invoice: {status} on {date}`

---

## Styling & Layout Recommendations

### CSS Grid Structure
```css
.milestone-tracker {
  display: grid;
  grid-template-columns: 200px repeat(auto-fit, minmax(40px, 1fr));
  gap: 1px;
  background-color: #f3f4f6;
  padding: 20px;
  border-radius: 8px;
  font-family: system-ui, -apple-system, sans-serif;
}

.milestone-row-label {
  padding: 12px;
  font-weight: 600;
  border-right: 2px solid #e5e7eb;
  background-color: #ffffff;
}

.week-cell {
  min-height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  border-radius: 4px;
  transition: all 0.2s ease;
  position: relative;
}

.week-cell:hover {
  transform: scale(1.1);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
  z-index: 10;
}

.week-header {
  padding: 8px;
  font-size: 12px;
  font-weight: 500;
  text-align: center;
  background-color: #ffffff;
  border-bottom: 2px solid #e5e7eb;
}

.month-header {
  padding: 12px 8px;
  font-weight: 700;
  font-size: 13px;
  color: #374151;
  border-bottom: 1px solid #d1d5db;
}
```

### Responsive Design
- **Desktop**: Show all weeks in grid
- **Tablet**: Show 12-16 weeks per view with horizontal scroll
- **Mobile**: Show 4-6 weeks per view with horizontal scroll

---

## Error Handling & Edge Cases

### Empty States
- If `practice` array is empty: Show "No practice milestones found"
- If `signoff` array is empty: Show "No signoff data available"
- If no weeks data: Show "Unable to calculate week timeline"

### Loading States
- Show skeleton loaders while fetching data
- Display loading spinner with "Fetching milestone data..."

### Error States
- Network error: "Failed to load milestone health. Please try again."
- Invalid project ID: "Project not found"
- Update failed: "Failed to update milestone. Please try again."

---

## Accessibility Requirements

- Use semantic HTML (`<button>`, `<div role="presentation">`)
- Add `aria-label` for cells: `"Week 1, Feb 3, On Track, Press to edit"`
- Ensure color is not the only indicator (use patterns/icons)
- Keyboard navigation: Tab through cells, Enter to edit
- WCAG 2.1 AA compliant contrast ratios

---

## Sample Implementation Checklist

- [ ] Fetch milestone health data from API
- [ ] Parse `all_weeks` to create week headers
- [ ] Render practice milestone rows with colored cells
- [ ] Render signoff milestone rows with circle markers
- [ ] Render invoice milestone rows with diamond markers
- [ ] Implement click handlers for each cell/marker
- [ ] Create status editor modal component
- [ ] Implement update API calls with optimistic updates
- [ ] Add error handling and loading states
- [ ] Add tooltips on hover
- [ ] Test responsive design on mobile/tablet
- [ ] Verify accessibility (WCAG compliance)
- [ ] Add animation for status transitions
- [ ] Cache data appropriately (revalidate on project change)

---

## Notes for Frontend Developer

1. **Date Handling**: All dates in API response are ISO 8601 format (UTC). Parse them carefully.
2. **Week Alignment**: The `week_label` in cells matches the Monday of each week, which aligns with `all_weeks` keys.
3. **Timezone**: Consider user's timezone for displaying dates (optional enhancement).
4. **Caching**: Implement smart cache invalidation when status is updated.
5. **Real-time Updates**: Consider adding WebSocket listener for real-time milestone updates (future enhancement).
6. **Performance**: For projects with 100+ weeks, consider virtual scrolling to optimize render performance.

