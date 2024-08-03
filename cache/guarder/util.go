package guarder

func Async(fn func()) {

	go func() {

		defer func() {
			if err := recover(); err != nil {

			}
		}()

		fn()
	}()
}
