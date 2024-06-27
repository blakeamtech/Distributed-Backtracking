package checkcostas

//import "fmt"

func IsConsistent(perm []int) bool {
	m := len(perm)
	for _, n := range perm {
		if n > m {
			m = n;
		}
	}
	vect := make([][]bool, 2 * m)
	for i, _ := range vect {
		vect[i] = make([]bool, 2 * m)
	}
	for i, x := range perm {
		for j, y := range perm {
			if (i == j) {
				continue
			}
			if vect[m + j - i][m + y - x] {
				return false
			}
			vect[m + j - i][m + y - x] = true
		}
	}
	return true
}
