package check

func IsConsistent(perm []int, prob string) bool {
	fmap := map[string]func([]int) bool {"costas" : IsConsistentCostas, "schur" : IsConsistentSchur}
	foo, ok := fmap[prob]
	if ok {
		return foo(perm)
	}
	return true;
}

func IsConsistentSchur(perm []int) bool {
	for i, n := range perm {
		for j := 0; j < i / 2; j++ {
			if j == i || (perm[j] == n && perm[i - j - 1] == n) {
				return false
			}
		}
	}
	return true
}

func IsConsistentCostas(perm []int) bool {
	m := len(perm)
	for _, n := range perm {
		if n > m {
			m = n
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