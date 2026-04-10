# Frontend: Milestone Health Tracker Implementation

## Task Summary
Implement an interactive milestone health tracker UI that displays practice milestones, client signoff, and invoice status across a week-based timeline grid (similar to GitHub contribution graph).

## API Integration

**Endpoint**: `GET /api/projects/{projectId}/milestone-health`

**Response Structure**:
```javascript
{
  "project_id": "string",
  "project_name": "string",
  "practice": [
    {
      "id": "uuid",                    // ✅ Milestone ID for API updates
      "milestone_code": "M1",
      "description": "string",
      "weeks": [
        {
          "week_number": 0,
          "week_label": "Jan 27, 2025",
          "status": "On Track|Completed|At Risk|Blocked",
          "color": "green|blue|orange|red|gray",
          "date": "ISO string"
        }
      ],
      "completion_pct": number,
      "status": "string",
      "start_date": "ISO string",
      "end_date": "ISO string"
    }
  ],
  "signoff": [ /* same structure with id */ ],
  "invoice": [ /* same structure with id */ ],
  "all_weeks": {
    "0": { "label": "Jan 27, 2025", "start": "ISO string" },
    "1": { "label": "Feb 3, 2025", "start": "ISO string" },
    // ... more weeks
  },
  "weeks_range": {
    "start_week": "Jan 27, 2025",
    "end_week": "Apr 6, 2026",
    "total_weeks": 63
  }
}
```

## UI Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ ITSE Dashboard - Milestone Health                               │
│ Last Updated: Apr 10, 2026                                      │
└─────────────────────────────────────────────────────────────────┘

Week Headers:  | Jan      | Feb      | Mar      | Apr      |
               | 1 2 3 4  | 1 2 3 4  | 1 2 3 4  | 1 2      |
               
PRACTICE       | [M1]     | [M1][M2] | [M2]     |          |
               | [M2]     | [empty]  | [empty]  | [M3]     |

SIGNOFF        |          | [●M1]    |          |          |
               |          | [●M2]    |          |          |

INVOICE        |          | [◆M1]    |          |[◆M2 Apr9]|
               |          |          |          |          |
```

## Data Mappings

### Practice Milestones
- **Cell Color**: Use `weeks[i].color` (green, blue, orange, red, gray)
- **Status Badge**: Show `weeks[i].status` on hover
- **Interaction**: Click cell → Open status editor modal
- **Display Range**: Show only weeks in `milestone.weeks` array

### Signoff Milestones  
- **Marker**: Circle (●) in the week calculated from `date` field
- **Color**: Green (●) if done, Orange (○) if pending
- **Placement**: Shows actual `signedoff_date` if Done, else `actual_end_eta` if Pending
- **Interaction**: Click marker → Open signoff status editor

### Invoice Milestones
- **Marker**: Diamond (◆) in the week calculated from `date` field  
- **Color**: Green (◆) if done, Orange (◇) if pending
- **Placement**: Shows actual `invoice_raised_date` if Done, else `actual_end_eta` if Pending
- **Interaction**: Click marker → Open invoice status editor

## Color Palette

```javascript
const colors = {
  "green": "#22c55e",    // On Track
  "blue": "#3b82f6",     // Completed
  "orange": "#f97316",   // At Risk / Pending
  "red": "#ef4444",      // Blocked
  "gray": "#d1d5db"      // Not Started
};
```

## Components to Build

### 1. MilestoneHealthTracker (Main Container)
- Fetch data from API on mount
- Handle loading/error states
- Render three sections: Practice, Signoff, Invoice
- Manage modal state for edits

### 2. WeekHeaderRow
- Parse `all_weeks` and group by month
- Display month names as merged headers
- Display week numbers below month names
- Each column = one week (40-50px wide)

### 3. PracticeRow (per milestone)
- Display milestone code + description
- Render colored cells for each week in `milestone.weeks`
- Show completion % and days variance
- Hover shows: `{week_label} - {status}`
- Click opens status editor

### 4. SignoffRow (per milestone)
- Display milestone code + description
- Render single marker (circle) in calculated week
- Color: green if done, orange if pending
- Hover shows: `Signoff: {status} on {date}`
- Click opens signoff editor

### 5. InvoiceRow (per milestone)
- Display milestone code + description
- Render single marker (diamond) in calculated week
- Color: green if done, orange if pending
- Hover shows: `Invoice: {status} on {date}`
- Click opens invoice editor

### 6. StatusEditorModal
- **For Practice**: Dropdown to select (On Track, At Risk, Blocked, Completed)
- **For Signoff**: Dropdown (Pending, Done) + date picker if Done
- **For Invoice**: Dropdown (Pending, Done) + date picker if Done
- Validate dates (must be ≤ today)
- On save: PATCH `/api/milestones/{id}` with updated fields
- On success: Refresh milestone data and close modal
- On error: Show error message

## Key Implementation Rules

1. **Week Calculation**: 
   - `week_number` in response already calculated
   - Use `all_weeks[week_number]` to find week details
   - `week_label` = Monday of that week for alignment

2. **Conditional Rendering**:
   - Practice: Show entire range from `start_date` to `end_date` weeks
   - Signoff: Show only single marker in one week (date-dependent)
   - Invoice: Show only single marker in one week (date-dependent)

3. **Status Change Logic**:
   - Practice: Update `status` field
   - Signoff: Update `client_signoff_status` and optionally `signedoff_date`
   - Invoice: Update `invoice_status` and optionally `invoice_raised_date`

4. **Responsive Design**:
   - Desktop: Show all weeks
   - Tablet/Mobile: Horizontal scroll for weeks
   - Consider virtual scrolling for 100+ weeks

5. **Accessibility**:
   - Use `aria-label` on cells: `"Week X, Feb 3, On Track, Press to edit"`
   - Keyboard navigation with Tab/Enter
   - Color + icons (not color alone)
   - WCAG 2.1 AA contrast ratios

## Error Handling

- Network error → "Failed to load milestone health"
- Update failed → "Failed to update milestone. Please try again"
- Invalid project → "Project not found"
- Show error toast/alert, not modal

## Optional Enhancements

- [ ] Real-time updates via WebSocket
- [ ] Bulk status updates for multiple weeks
- [ ] Export timeline as image/PDF
- [ ] Filter milestones by status
- [ ] Add milestone notes/comments
- [ ] Dark mode support
- [ ] Animated transitions on status change

## Testing Checklist

- [ ] Fetch and parse API response correctly
- [ ] All three sections (practice, signoff, invoice) render
- [ ] Week headers align with grid columns
- [ ] Click on cells opens editor modal
- [ ] Modal saves changes and refreshes data
- [ ] Responsive behavior works on mobile
- [ ] Loading state shows while fetching
- [ ] Error state displays on failure
- [ ] Hover tooltips show correct info
- [ ] Keyboard navigation works

---

## Quick Start

```javascript
// Example: Fetching milestone health
const fetchMilestoneHealth = async (projectId) => {
  const response = await fetch(`/api/projects/${projectId}/milestone-health`);
  const data = await response.json();
  
  // data.practice = array of milestones with weeks
  // data.signoff = array of signoff milestones
  // data.invoice = array of invoice milestones
  // data.all_weeks = { "0": {...}, "1": {...}, ... }
  // data.weeks_range = { start_week, end_week, total_weeks }
  
  return data;
};

// Example: Updating milestone status
const updateMilestoneStatus = async (milestoneId, updates) => {
  const response = await fetch(`/api/milestones/${milestoneId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(updates)
  });
  return response.json();
};
```
