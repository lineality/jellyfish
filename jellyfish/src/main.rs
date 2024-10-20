/*
Do One Thing Well

few large process, post: fiddler_crab
many small_processes, post: jellyfish
get requests: get_tui


*/
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::collections::VecDeque;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex};

const MAX_QUEUE_SIZE: usize = 500;
const PROCESSING_DELAY_MS: u64 = 100; // Adjust as needed
const REQUEST_HANDLER_PAUSE: u64 = 10; // millis

// Define a struct to represent a job that the thread pool can execute
struct Job(Box<dyn FnOnce() + Send + 'static>);

// Define the ThreadPool struct
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Sender<Job>,
}

// Define the Worker struct
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>, // Make thread optional
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    println!("Worker {id} got a job; executing.");

                    job.0(); // Execute the job
                }
                Err(_) => {
                    println!("Worker {id} disconnected; shutting down.");
                    break; // Exit the loop if the receiver is disconnected
                }
            }
        });

        Worker {
            id,
            thread: Some(thread), // Store the thread handle
        }
    }
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.send(Job(job)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers");

        for _ in &self.workers {
            self.sender.send(Job(Box::new(|| println!("Terminating")))).unwrap();
        }

        println!("Shutting down all workers.");

        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}


// For states of request_hanlder
enum HandlerState {
    // Busy,
    Idle,
    Failed,
}

/// Represents the state of the request handler with Integer Mapping
/// 
/// This atomic variable is used to track whether the handler is currently busy processing a request,
/// idle and available to handle a new request, or in a failed state.
/// 
/// The state is represented as a `usize` to be compatible with the `AtomicUsize` type.
/// The possible states are defined by the `HandlerState` enum:
/// - `Busy`: The handler is currently processing a request.
/// - `Idle`: The handler is available to process a new request.
/// - `Failed`: The handler has encountered an error and is not operational.
/// 
/// The initial state is set to `Idle`.
/// AtomicUsize
///
/// Represents an unsigned integer (usize) that can be safely accessed and modified by multiple threads concurrently.
///
/// It provides atomic operations (e.g., load, store, compare-and-swap) 
/// that guarantee that these operations are completed as a single, indivisible unit, preventing race conditions.
///
/// It is not designed to store strings directly.
///
/// Why usize for HANDLER_STATE?
/// In the previous code, we used AtomicUsize to represent the HANDLER_STATE because:
/// 1. Enum Representation: We defined the HandlerState enum with different states (Busy, Idle, Failed).
/// 2. Integer Mapping: We implicitly mapped these enum variants to integer values 
/// (e.g., Idle might be 0, Busy might be  1, and Failed might be 2). This mapping is done automatically by the compiler 
/// when you cast an enum to an integer (e.g., HandlerState::Idle as usize).
/// 3. Atomic Storage: We needed an atomic variable to store this integer representation of the state 
/// so that multiple threads could safely access and update it. 
/// AtomicUsize is suitable because it can store unsigned integers.
static HANDLER_STATE: AtomicUsize = AtomicUsize::new(HandlerState::Idle as usize); 

#[derive(Clone, Debug)]
struct RequestUnit {
    id: usize,
    body: String,
    output_for_response: Option<String>,
    stream_addr: std::net::SocketAddr, // Or a unique stream ID
    response_status: Option<u16>, 
    response_headers: Option<Vec<(String, String)>>,
    response_body: Option<String>,
}

// for processing and functions on request data
fn process_a_request(mut request_unit_struct: RequestUnit) -> Result<RequestUnit, String> {
    // TODO: Implement calling external program (e.g., Python, llama.cpp)
    println!("Processing request: {:?}", request_unit_struct);
    
    // 1. select parse_preprocess_function()
    // e.g. extract and read fields from json
    // process header, etc.
    
    // 2. select/run output_function()
    let result_of_function = request_unit_struct.body.clone(); // ... get output from external program ...

    // 3. Set up Response Data   
    request_unit_struct.response_status = Some(200); 
    request_unit_struct.response_headers = Some(vec![("Content-Type".to_string(), "text/plain".to_string())]); 
    request_unit_struct.response_body = Some(result_of_function);

        
    // intentional delay
    thread::sleep(Duration::from_millis(PROCESSING_DELAY_MS));
    
    // Return the modified RequestUnit or an error message
    if /* processing successful */ true {
        Ok(request_unit_struct) // Return the RequestUnit directly
    } else {
        Err("Processing failed".to_string())
    }
}



/// Jellyfish: process each queue item in a concurrent thread
/// Handles a request and a queue of requests concurrently using a thread pool.
///
/// This function represents the core logic of the Jellyfish server's request 
/// handling. It takes an initial request, a queue of pending requests, a 
/// channel sender to communicate results back to the main thread, and a 
/// thread pool to manage concurrent processing.
///
/// # Arguments
///
/// * `request_unit_struct`: The initial RequestUnit to be processed.
/// * `disposable_handoff_queue`: A mutable VecDeque containing pending RequestUnits.
/// * `sender`: A Sender to communicate the results (processed RequestUnit or error) back to the main thread.
/// * `pool`: A reference to the ThreadPool used to manage concurrent execution of requests.
///
/// # Behavior
///
/// 1. **Adds the initial request to the queue:** The `request_unit_struct` is 
///    added to the `disposable_handoff_queue`.
/// 2. **Processes the queue concurrently:** 
///    - Enters a loop that continuously checks for requests in the queue.
///    - If the queue is not empty, it pops a RequestUnit from the front.
///    - It then spawns a new thread (using the `pool`) to process the 
///      RequestUnit concurrently.
///    - The spawned thread executes the `process_a_request` function and sends 
///      the result (processed RequestUnit or error message) back to the main 
///      thread using the `sender`.
///    - If the queue is empty, it pauses briefly before checking again.
/// 3. **Handles panics:** The entire closure is wrapped in `catch_unwind` to 
///    handle potential panics within the spawned threads. If a panic occurs, 
///    the `HANDLER_STATE` is set to `Failed`, and a message is printed. 
///    (Note: Error handling within the panicked thread should be improved).
///
/// # Concurrency
///
/// This function leverages a thread pool to process queue items concurrently. 
/// Each request in the queue is handled by a separate thread, potentially 
/// improving performance, especially for smaller, faster requests.
///
/// # Error Handling
///
/// Basic error handling is implemented using `Result` and the `sender` channel 
/// to communicate errors back to the main thread. However, more robust error 
/// handling within the spawned threads (e.g., handling specific error types, 
/// logging, retry mechanisms) might be needed in a production environment. 
/// The `catch_unwind` block provides a basic safety net for panics, but it 
/// should be augmented with more specific panic handling strategies.
fn concurrent_handler_of_request_queue(
    request_unit_struct: RequestUnit,
    mut disposable_handoff_queue: VecDeque<RequestUnit>,
    sender: Sender<(usize, Result<RequestUnit, String>)>,
    pool: &ThreadPool, // Pass the thread pool 
) {
    // Wrap the closure in AssertUnwindSafe
    let closure = AssertUnwindSafe(|| {
        // 1. Add the initial request to the queue
        disposable_handoff_queue.push_back(request_unit_struct);

        // 2. Process the queue concurrently
        loop {
            if let Some(request_unit) = disposable_handoff_queue.pop_front() {
                // Spawn a new thread (or use the thread pool) for each request
                let sender_clone = sender.clone(); // Clone the sender for each thread
                pool.execute(move || {
                    // Process the request and handle the result
                    match process_a_request(request_unit.clone()) {
                        Ok(processed_request) => {
                            // Send the processed RequestUnit back to the main thread
                            if let Err(e) = sender_clone.send((processed_request.id, Ok(processed_request))) {
                                eprintln!("Error sending processed request to main thread: {}", e);
                                // TODO: Handle the error appropriately (e.g., log, retry, or exit)
                            }
                        }
                        Err(error_message) => {
                            // Send the error message back to the main thread
                            if let Err(e) = sender_clone.send((request_unit.id, Err(error_message))) {
                                eprintln!("Error sending error message to main thread: {}", e);
                                // TODO: Handle the error appropriately (e.g., log, retry, or exit)
                            }
                        }
                    }
                });
            } else {
                // Queue is empty, wait a bit before checking again
                thread::sleep(Duration::from_millis(REQUEST_HANDLER_PAUSE));
            }
        }
    });

    // Call catch_unwind with the wrapped closure
    std::panic::catch_unwind(closure).unwrap_or_else(|_| {
        HANDLER_STATE.store(HandlerState::Failed as usize, Ordering::Relaxed);
        println!("Handler thread panicked!");
        // You'll need to return a RequestUnit here as well
    });
}



// TODO Add explanation here, in detail
/*
text
*/
static QUEUE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static REQUEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);


fn main() {
    
    // Create the thread pool (e.g., with 4 worker threads)
    // Create the thread pool and wrap it in Arc
    let pool = Arc::new(ThreadPool::new(40));
    
    // Main loop for crash resistance, 'Let it fail, and try again.'
    // Main Loop:
    // Purpose: The main loop is responsible for the overall lifecycle of the server. 
    // It initializes components, starts the stream-loop, and handles potential 
    // restarts if the stream-loop encounters errors.
    //
    // Execution: The main loop typically runs only once when the server starts and continues 
    // running indefinitely until the server is intentionally shut down.
    //
    // Responsibility for Queues: The main loop is responsible for creating the initial disposable handoff 
    // queue when the server starts. It might also handle the creation of a new queue if the handler thread 
    // encounters an error, but this logic might also be delegated to the stream-loop.
    loop {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        // TODO error handling, when fails: signal_to_restart = True, restart loop

        // Create a channel for communication between the main thread and the handler thread
        let (sender, receiver): (Sender<(usize, Result<RequestUnit, String>)>, Receiver<(usize, Result<RequestUnit, String>)>) = std::sync::mpsc::channel();

        // Create a mapping to store streams by request ID
        let mut stream_map: HashMap<usize, TcpStream> = HashMap::new();

        // Clone the sender before entering the loop
        let sender_clone = sender.clone();

        
        // Purpose: The stream-loop is responsible for listening for incoming requests, 
        // handling the request queue, and passing requests to the handler.
        // Execution: The stream-loop runs continuously within the main loop, 
        // accepting and processing incoming requests.
        // Responsibility for Queues: The stream-loop is primarily responsible 
        // for creating new disposable handoff queues immediately after handing 
        // off the previous queue to the handler. It also manages adding requests 
        // to the current queue and checking if the queue is full.
        // Additionally, the stream-loop can signal a restart of the main loop in case of bad failures.
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    
                    // Initial creation (in the main loop)
                    let mut disposable_handoff_queue: Option<VecDeque<RequestUnit>> = Some(VecDeque::with_capacity(MAX_QUEUE_SIZE));
                
                    let mut buffer = [0; 1024];
                    stream.read(&mut buffer).unwrap();
                    // TODO when fails, error restart_signal = True / or drop and continue stream

                    let request_string = String::from_utf8_lossy(&buffer[..]);
                    // TODO when fails, error restart_signal = True / or drop and continue stream

                    // Very basic parsing of the request (assuming POST)
                    if request_string.starts_with("POST") {
                        // let body_start = request_string.find("\r\n\r\n").unwrap_or(0) + 4;
                        // let request_body = request_string[body_start..].to_string();
                        let body_start = match request_string.find("\r\n\r\n") {
                            Some(index) => index + 4,
                            None => {
                                eprintln!("Error: Invalid request format. Could not find end of headers.");
                                // Handle the error appropriately (e.g., return an error response to the client)
                                // For now, let's just return 0 to avoid a crash, but you should replace this 
                                // with more robust error handling
                                0 
                            }
                        };
                        
                        let request_body = request_string[body_start..].to_string();
                        // TODO when fails, error restart_signal = True / or drop and continue stream

                        // Generate a unique request ID
                        let request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                    
                        // Stream Decoupling: Store stream address in RequestUnit
                        let stream_addr = stream.peer_addr().unwrap(); 
                        let request_unit_struct = RequestUnit {
                            id: request_id,
                            body: request_body,
                            output_for_response: None,
                            stream_addr: stream_addr,
                            response_status: None, // Initialize response fields to None
                            response_headers: None,
                            response_body: None,
                        };
                        
                        // Insert the stream into the map
                        stream_map.insert(request_id, stream);

                        // if Idle
                        // Checks if the request handler is currently in the `Idle` state.
                        //
                        // This function loads the current state of the handler from the `HANDLER_STATE` atomic variable
                        // and compares it with the integer representation of the `Idle` state. 
                        // It returns `true` if the handler is `Idle` and `false` otherwise (if it's `Busy` or `Failed`).
                        //
                        // Note: This check only explicitly distinguishes between `Idle` and non-`Idle` states.
                        // It does not differentiate between `Busy` and `Failed` within this specific check. 
                        // However, the `else` block that follows this check handles both `Busy` and `Failed` states 
                        // by attempting to add the incoming request to the queue.
                        if HANDLER_STATE.load(Ordering::Relaxed) == HandlerState::Idle as usize {
                            /*
                            handler can be: 1 busy, 2. not_busy 3. failed
                            
                            A. look for quit-signal_to_restart (optional, if needed later)
                            B. if handler is not busy, give request+queue to handler & reset counter to 0
                            C. if handler is busy, check counter
                            E. if counter > MAX: drop request
                            F. if counter < MAX: check if there is an existing queue
                            G. if there is an existing queue: add request to quque
                            H: if there is no queue: make a queue and add request to queue
                            loop back 
                            */
                            // Request processing oc
                            // when this fails (everything will fail at some point)
                            // this should output a signal to set a 'restart' flag 
                            // Spawn the handler thread and pass the sender channel
                            // In main
                            
                            // Clone sender_clone inside the loop
                            let sender_for_thread = sender_clone.clone(); 
                            let pool_clone = Arc::clone(&pool); // Clone the Arc for the closure
                            
                            thread::spawn(move || {
                                concurrent_handler_of_request_queue(
                                    request_unit_struct,
                                    disposable_handoff_queue.take().unwrap(),
                                    sender_for_thread, // Pass a clone of sender_clone
                                    &pool_clone, // Pass the thread pool to the handler
                                );
                            });

                            // 1. concurrent_handler_of_request_queue(request, quque)

                            // Double Tap: make sure queue is removed
                            // When the handler finishes or fails (in the handler thread or stream-loop):
                            // let disposable_handoff_queue: Option<VecDeque<String>> = None; // Indicate that a new queue needs to be created 
                            
                            // 2. counter = zero
                            // Reset the queue counter
                            QUEUE_COUNTER.store(0, Ordering::Relaxed);

                            // 3. make a new empty disposable_handoff_queue
                            // let mut disposable_handoff_queue: Option<VecDeque<String>> = Some(VecDeque::with_capacity(MAX_QUEUE_SIZE));
                            
                            // if faile:
                            // exit/continue/break stream-loop/quit/reboot
                            
                            
                        } else {  // if NOT Idle: elif busy, elif failed
                            
                            // Handle busy/failed state (e.g., add to queue)
                            // Check if a queue exists and add requests to it
                            if let Some(queue) = &mut disposable_handoff_queue {
                                // ... (add requests to the queue) ...
                                // if queue is not full: add request to queue
                                if QUEUE_COUNTER.load(Ordering::Relaxed) < MAX_QUEUE_SIZE {

                                    // add request to queue!
                                    queue.push_back(request_unit_struct); // Call push_back on the VecDeque inside the Option
                     
                                    QUEUE_COUNTER.fetch_add(1, Ordering::Relaxed);
                            } else {
                                // No queue available, create a new one 
                                // let mut disposable_handoff_queue: Option<VecDeque<String>> = Some(VecDeque::with_capacity(MAX_QUEUE_SIZE));
                                // ... (potentially add the current request to the new queue) ...
                            }
                            

                            } else {
                                // Ignore the request (queue is full)
                            }
                            
                            // increment counter
                            QUEUE_COUNTER.fetch_add(1, Ordering::Relaxed);
                        }
                        
                        
                        if HANDLER_STATE.load(Ordering::Relaxed) == HandlerState::Failed as usize {
                            println!("Handler thread failed. Restarting..."); // Log the failure
                            break; // Exit the stream-loop to signal a restart 
                        }

                    }

                    // (maybe too old of a text comment block)4. Respond
                    /*
                    Check for Valid Response Data: We first check if the response_status, response_headers, 
                    and response_body fields in the RequestUnit have been populated.
                
                    Establish Connection: We use TcpStream::connect to establish a new connection 
                    to the client using the stream_addr stored in the RequestUnit.
                
                    Send Status Line: We construct the HTTP status line (e.g., "HTTP/1.1 200 OK\r\n") 
                    and send it through the stream.
                
                    Send Headers: We iterate through the response_headers and send each 
                    header line (e.g., "Content-Type: text/plain\r\n").
                
                    Send Empty Line: We send an empty line ("\r\n") to indicate the end of the headers.
                
                    Send Response Body: Finally, we send the response_body through the stream.
                    */
                    // 4?
                    // Receive the processed RequestUnit or error from the channel
                    if let Ok((request_id, result)) = receiver.recv() {
                        // Find the corresponding stream using the request ID
                        if let Some(mut stream) = stream_map.remove(&request_id) {
                            // Handle the result from the handler
                            match result {
                                Ok(processed_request) => {
                                    // Send successful response
                                    let response = format!(
                                        "HTTP/1.1 {} OK\r\nContent-Type: text/plain\r\n\r\n{}",
                                        processed_request.response_status.unwrap_or(200), // Get status or default to 200
                                        processed_request.response_body.unwrap_or_default() // Get body or default to empty
                                    );
                                    stream.write_all(response.as_bytes()).unwrap();
                                    stream.flush().unwrap(); // Flush the stream to ensure data is sent
                                }
                                Err(error_message) => {
                                    // Send error response
                                    let response = format!("HTTP/1.1 500 Internal Server Error\r\n\r\n{}", error_message);
                                    stream.write_all(response.as_bytes()).unwrap();
                                    stream.flush().unwrap(); // Flush the stream
                                }
                            }
                        } else {
                            eprintln!("Stream not found for request ID: {}", request_id);
                            // Handle the case where the stream is not found
                        }
                    } else {
                        eprintln!("Error receiving data from handler thread.");
                        // Handle the error appropriately
                    }
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
                // look for restart-flag from failure and signal larger restart exit
            }
        }

        // If the code reaches here, it means the listener loop has exited (e.g., due to an error)
        // The outer loop will restart, creating a fresh disposable_handoff_queue and listener
    } 
}
