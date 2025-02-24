package journal

type SetOptions struct {
	Concurrency int
}

type Set struct {
	shutdownc chan struct{}
}

func StartSet(opt SetOptions) *Set {
	return &Set{}
}

func (set *Set) Close() {

}

func (set *Set) Add(j *Journal) {
}

func (set *Set) Remove(j *Journal) {
}
