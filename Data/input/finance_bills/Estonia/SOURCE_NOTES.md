# Estonia Source Notes

## Source Family
- Annual state budget law PDFs
- Mostly one file per year
- Compact corpus, likely manageable in one overnight batch

## File Coverage
- Current folder begins at `1991`
- Appears continuous into the 2000s and later

## Language / Number Format
- Estonian
- Expect European number formatting
- Important era break:
  - pre-euro: kroon / EEK
  - euro era later on

## Likely R&D Actors
- education / research ministry appropriations
- Academy / research council equivalents
- universities and national research institutes
- innovation and technology support lines

## Likely False Positives
- broad education funding
- municipal transfers
- social insurance
- transport / infrastructure

## First Audit Targets
- `1991 13111571.pdf`
- `1999.pdf`
- `2004 822726.pdf`
- `2009 13111066.pdf`
- one recent file

## Extraction Notes
- Explicitly document the currency break in config/profile
- Confirm whether units are full currency units or scaled in the law header
- Check if research appears as ministries, agencies, or university block grants

