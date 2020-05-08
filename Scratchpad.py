class Solution:
    def checkRecord(self, s: str) -> bool:
        count_absent = 0
        count_late = 0

        for i in range(len(s)):
            print(s[i])
            if s[i] == 'A':
                count_absent += 1
            if s[i] == 'L' and i < len(s) - 1:
                if s[i + 1] == 'L':
                    count_late += 1
            print(count_late, count_absent)
        return not (count_absent > 1 or count_late > 2)

sol = Solution()

print(sol.checkRecord("PPALLP"))