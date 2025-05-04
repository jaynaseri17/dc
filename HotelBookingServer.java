import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class HotelBookingServer extends UnicastRemoteObject implements HotelBooking {
    private Map<Integer, String> bookings; // room number to guest name mapping
    private static final int TOTAL_ROOMS = 10; // assuming hotel has 10 rooms
    
    public HotelBookingServer() throws RemoteException {
        super();
        bookings = new HashMap<>();
    }
    
    @Override
    public String bookRoom(String guestName, int roomNumber) throws RemoteException {
        if (roomNumber < 1 || roomNumber > TOTAL_ROOMS) {
            return "Invalid room number. Please choose between 1 and " + TOTAL_ROOMS;
        }
        
        if (bookings.containsKey(roomNumber)) {
            return "Room " + roomNumber + " is already booked by " + bookings.get(roomNumber);
        }
        
        bookings.put(roomNumber, guestName);
        return "Room " + roomNumber + " successfully booked for " + guestName;
    }
    
    @Override
    public String cancelBooking(String guestName) throws RemoteException {
        for (Map.Entry<Integer, String> entry : bookings.entrySet()) {
            if (entry.getValue().equals(guestName)) {
                bookings.remove(entry.getKey());
                return "Booking canceled for " + guestName;
            }
        }
        return "No booking found for " + guestName;
    }
    
    @Override
    public String listBookings() throws RemoteException {
        if (bookings.isEmpty()) {
            return "No current bookings";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Current Bookings:\n");
        for (Map.Entry<Integer, String> entry : bookings.entrySet()) {
            sb.append("Room ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
    
    public static void main(String[] args) {
        try {
            // Create and export the server object
            HotelBookingServer server = new HotelBookingServer();
            
            // Bind the server object to the RMI registry
            java.rmi.Naming.rebind("HotelBookingService", server);
            
            System.out.println("Hotel Booking Server is ready...");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}