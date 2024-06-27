package checkschur

//import "fmt"

func IsConsistent(perm []int) bool {
	for i, n := range perm {
		for j := 0; j < i / 2; j++ {
			if j == i || (perm[j] == n && perm[i - j - 1] == n) {
				return false
			}
		}
	}
	return true
}
