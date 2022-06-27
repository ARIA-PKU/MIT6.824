package mr

// restore some common values and usages
import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	MaxTaskTime = time.Second * 10
)

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finished
)

type OperationPhase uint8

const (
	MapPhase OperationPhase = iota
	ReducePhase
	CompletedPhase
)

type WorkType uint8

const (
	Map WorkType = iota
	Reduce
	Wait
	Completed
)


// generate the name like mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func generateMapFilePath(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%v-%v", mapNumber, reduceNumber)
}

//  the output of the X'th reduce task in the file mr-out-X
func generateReduceFilePath(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%v", reduceNumber)
}

// store content to file atomically
func atomicWriteFile(filename string, r io.Reader) (err error) {
	// write to a temp file first, then we'll atomically replace the target file
	// with the temp file.
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	// create temp file
	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}

	// delete the file name
	defer func() {
		if err != nil {
			// Don't leave the temp file lying around on error.
			_ = os.Remove(f.Name()) // yes, ignore the error, not much we can do about it.
		}
	}()
	
	// ensure we always close f. Note that this does not conflict with  the
	// close below, as close is idempotent.
	defer f.Close()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}

	// get the file mode from the original file and use that for the replacement
	// file, too.
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// no original file
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}